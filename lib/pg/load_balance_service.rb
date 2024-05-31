# frozen_string_literal: true
require 'pg' unless defined?( PG )
require 'concurrent'

class PG::LoadBalanceService

  LBProperties = Struct.new(:placements_info, :refresh_interval, :fallback_to_tk_only, :failed_host_reconnect_delay)
  Node = Struct.new(:host, :port, :cloud, :region, :zone, :public_ip, :count, :is_down, :down_since)
  CloudPlacement = Struct.new(:cloud, :region, :zone)
  @@mutex = Concurrent::ReentrantReadWriteLock.new
  @@last_refresh_time = -1
  @@control_connection = nil
  @@cluster_info = { }
  @@useHostColumn = nil

  def self.get_load(host)
    if @@cluster_info[host]
      @@cluster_info[host].count
    else
      0
    end
  end

  def self.decrement_connection_count(host)
    @@mutex.acquire_write_lock
    begin
      info = @@cluster_info[host]
      unless info.nil?
        info.count -= 1
        if info.count < 0
          # Can go negative if we are here because of a connection that was created in a non-LB fashion
          info.count = 0
        end
        return true
      end
    ensure
      @@mutex.release_write_lock
    end
    false
  end

  def self.connect_to_lb_hosts(lb_props, iopts)
    refresh_done = false
    @@mutex.acquire_write_lock
    begin
      if metadata_needs_refresh lb_props.refresh_interval
        while !refresh_done
          if @@control_connection == nil
            @@control_connection = create_control_connection(iopts)
          end
          begin
            refresh_yb_servers(lb_props.failed_host_reconnect_delay, @@control_connection)
            refresh_done = true
          rescue => err
            if iopts[:host] == @@control_connection.host
              if @@cluster_info[iopts[:host]]
                @@cluster_info[iopts[:host]].is_down = true
                @@cluster_info[iopts[:host]].down_since = Time.now.to_i
              end

              new_list = @@cluster_info.select {|k, v| !v.is_down }
              if new_list.length > 0
                h = new_list.keys.first
                iopts[:port] = new_list[h].port
                iopts[:host] = h
              else
                return nil
                # raise(PG::Error, "Unable to create a control connection")
              end
            end
            @@control_connection = create_control_connection(iopts)
          end
        end
      end
    ensure
      @@mutex.release_write_lock
    end
    success = false
    new_request = true
    placement_index = 1
    until success
      @@mutex.acquire_write_lock
      begin
        host_port = get_least_loaded_server(lb_props.placements_info, lb_props.fallback_to_tk_only, new_request, placement_index)
        new_request = false
      ensure
        @@mutex.release_write_lock
      end
      unless host_port
        break
      end
      lb_host = host_port[0]
      lb_port = host_port[1]
      placement_index = host_port[2]
      if lb_host.empty?
        break
      end
      # modify iopts args
      begin
        iopts[:host] = lb_host
        iopts[:port] = lb_port
        # iopts = resolve_hosts(iopts)
        connection = PG.connect(iopts)
        success = true
      rescue => e
        @@mutex.acquire_write_lock
        begin
          @@cluster_info[lb_host].is_down = true
          @@cluster_info[lb_host].down_since = Time.now.to_i
          @@cluster_info[lb_host].count -= 1
          if @@cluster_info[lb_host].count < 0
            @@cluster_info[lb_host].count = 0
          end
        ensure
          @@mutex.release_write_lock
        end
      end
    end
    connection
  end

  def self.create_control_connection(iopts)
    conn = nil
    success = false
    # Iterate until control connection is successful or all nodes are tried
    until success
      begin
        conn = PG.connect(iopts)
        success = true
      rescue => e
        if @@cluster_info[iopts[:host]]
          @@cluster_info[iopts[:host]].is_down = true
          @@cluster_info[iopts[:host]].down_since = Time.now.to_i
        end

        new_list = @@cluster_info.select {|k, v| !v.is_down }
        if new_list.length > 0
          h = new_list.keys.first
          iopts[:port] = new_list[h].port
          iopts[:host] = h
        else
          raise(PG::Error, "Unable to create a control connection")
        end
      end
    end
    conn
  end

  def self.refresh_yb_servers(failed_host_reconnect_delay_secs, conn)
    rs = conn.exec("select * from yb_servers()")
    found_public_ip = false
    rs.each do |row|
      # Take the first address of resolved host addresses
      host = resolve_host(row['host'])[0][0] # 2D array
      port = row['port']
      cloud = row['cloud']
      region = row['region']
      zone = row['zone']
      public_ip = row['public_ip']
      public_ip = resolve_host(public_ip)[0][0] if public_ip
      if not public_ip.nil? and not public_ip.empty?
        found_public_ip = true
      end

      # todo set useHostColumn field
      if @@useHostColumn.nil?
        if host.eql? conn.host
          @@useHostColumn = true
        end
        if !public_ip.nil? && (public_ip.eql? conn.host)
          @@useHostColumn = false
        end
      end
      old = @@cluster_info[host]
      if old
        if old.is_down
          if Time.now.to_i - old.down_since > failed_host_reconnect_delay_secs
            old.is_down = false
          end
          @@cluster_info[host] = old
        end
      else
        node = Node.new(host, port, cloud, region, zone, public_ip, 0, false, 0)
        @@cluster_info[host] = node
      end
    end
    if @@useHostColumn.nil?
      if found_public_ip
        @@useHostColumn = false
      end
    end
    @@last_refresh_time = Time.now.to_i
  end

  def self.get_least_loaded_server(allowed_placements, fallback_to_tk_only, new_request, placement_index)
    current_index = 1
    selected = Array.new
    unless allowed_placements.nil? # topology-aware
      eligible_hosts = Array.new
      (placement_index..10).each { |idx|
        current_index = idx
        selected.clear
        min_connections = 1000000 # Using some really high value
        @@cluster_info.each do |host, node_info|
          unless node_info.is_down
            unless allowed_placements[idx].nil?
              allowed_placements[idx].each do |cp|
                if cp[0] == node_info.cloud && cp[1] == node_info.region && (cp[2] == node_info.zone || cp[2] == "*")
                  eligible_hosts << host
                  if node_info.count < min_connections
                    min_connections = node_info.count
                    selected.clear
                    selected.push(host)
                  elsif node_info.count == min_connections
                    selected.push(host)
                  end
                  break # Move to the next node
                end
              end
            end
          end
        end
        if selected.length > 0
          break
        end
      }
    end

    if allowed_placements.nil? || (selected.empty? && !fallback_to_tk_only) # cluster-aware || fallback_to_tk_only = false
      unless allowed_placements.nil?
      end
      min_connections = 1000000 # Using some really high value
      selected = Array.new
      @@cluster_info.each do |host, node_info|
        unless node_info.is_down
          if node_info.count < min_connections
            min_connections = node_info.count
            selected.clear
            selected.push(host)
          elsif node_info.count == min_connections
            selected.push(host)
          end
        end
      end
    end

    if selected.empty?
      nil
    else
      index = rand(selected.size)
      selected_node = selected[index]
      @@cluster_info[selected_node].count += 1
      if !@@useHostColumn.nil? && !@@useHostColumn
        selected_node = @@cluster_info[selected_node].public_ip
      end
      Array[selected_node, @@cluster_info[selected_node].port, current_index]
    end
  end

  def self.parse_lb_args_from_url(conn_string)
    string_parts = conn_string.split('?', -1)
    if string_parts.length != 2
      return conn_string, nil
    else
      base_string = string_parts[0] + "?"
      lb_props = Hash.new
      tokens = string_parts[1].split('&', -1)
      tokens.each {
        |token|
        unless token.empty?
          k, v = token.split('=', 2)
          case k
          when "load_balance"
            lb_props[:load_balance] = v
          when "topology_keys"
            lb_props[:topology_keys] = v
          when "yb_servers_refresh_interval"
            lb_props[:yb_servers_refresh_interval] = v
          when "failed_host_reconnect_delay_secs"
            lb_props[:failed_host_reconnect_delay_secs] = v
          when "fallback_to_topology_keys_only"
            lb_props[:fallback_to_topology_keys_only] = v
          else
            # not LB-specific
            base_string << "#{k}=#{v}&"
          end
        end
      }

      base_string = base_string.chop if base_string[-1] == "&"
      base_string = base_string.chop if base_string[-1] == "?"
      if not lb_props.empty? and lb_props[:load_balance].to_s.downcase == "true"
        return base_string, parse_connect_lb_args(lb_props)
      else
        return base_string, nil
      end
    end
  end

  def self.parse_connect_lb_args(hash_arg)
    lb = hash_arg.delete(:load_balance)
    tk = hash_arg.delete(:topology_keys)
    ri = hash_arg.delete(:yb_servers_refresh_interval)
    ttl = hash_arg.delete(:failed_host_reconnect_delay_secs)
    fb = hash_arg.delete(:fallback_to_topology_keys_only)

    if lb && lb.to_s.downcase == "true"
      lb_properties = LBProperties.new(nil, 300, false, 5)
      if tk
        lb_properties.placements_info = Hash.new
        tk_parts = tk.split(',', -1)
        tk_parts.each {
          |single_tk|
          if single_tk.empty?
            raise ArgumentError, "Empty value for topology_keys specified"
          end
          single_tk_parts = single_tk.split(':', -1)
          if single_tk_parts.length > 2
            raise ArgumentError, "Invalid preference value '#{single_tk_parts}' specified for topology_keys: " + tk
          end
          cp = single_tk_parts[0].split('.', -1)
          if cp.length != 3
            raise ArgumentError, "Invalid cloud placement value '#{single_tk_parts[0]}' specified for topology_keys: " + tk
          end
          preference_value = 1
          if single_tk_parts.length == 2
            preference = single_tk_parts[1]
            if preference == ""
              raise ArgumentError, "No preference value specified for topology_keys: " + tk
            end
            begin
              preference_value = Integer(preference).to_i
            rescue
              raise ArgumentError, "Invalid preference value '#{preference}' for topology_keys: " + tk
            ensure
              if preference_value < 1 || preference_value > 10
                raise ArgumentError, "Invalid preference value '#{preference_value}' for topology_keys: " + tk
              end
            end
          end
          unless lb_properties.placements_info[preference_value]
            lb_properties.placements_info[preference_value] = Set.new
          end
          lb_properties.placements_info[preference_value] << CloudPlacement.new(cp[0], cp[1], cp[2])
        }
      end

      begin
        lb_properties.refresh_interval = Integer(ri).to_i if ri
      rescue ArgumentError => ae
        lb_properties.refresh_interval = 300
      ensure
        if lb_properties.refresh_interval < 0 || lb_properties.refresh_interval > 600
          lb_properties.refresh_interval = 300
        end
      end

      begin
        lb_properties.failed_host_reconnect_delay = Integer(ttl).to_i if ttl
      rescue ArgumentError
      ensure
        if lb_properties.failed_host_reconnect_delay < 0 || lb_properties.failed_host_reconnect_delay > 60
          lb_properties.failed_host_reconnect_delay = 5
        end
      end

      lb_properties.fallback_to_tk_only = fb.to_s.downcase == "true" if fb

    else
      lb_properties = nil
    end
    lb_properties
  end

  def self.metadata_needs_refresh(refresh_interval)
    if Time.now.to_i - @@last_refresh_time >= refresh_interval # || force_refresh == true
      true
    else
      false
    end
  end

  def self.resolve_host(mhost)
    if PG::Connection.host_is_named_pipe?(mhost)
      # No hostname to resolve (UnixSocket)
      hostaddrs = [nil]
    else
      if Fiber.respond_to?(:scheduler) &&
        Fiber.scheduler &&
        RUBY_VERSION < '3.1.'

        # Use a second thread to avoid blocking of the scheduler.
        # `TCPSocket.gethostbyname` isn't fiber aware before ruby-3.1.
        hostaddrs = Thread.new { Addrinfo.getaddrinfo(mhost, nil, nil, :STREAM).map(&:ip_address) rescue [''] }.value
      else
        hostaddrs = Addrinfo.getaddrinfo(mhost, nil, nil, :STREAM).map(&:ip_address) rescue ['']
      end
    end
    hostaddrs.map { |hostaddr| [hostaddr, mhost] }
  end

end
