# frozen_string_literal: true
require 'pg' unless defined?( PG )
require 'concurrent'

class PG::LoadBalanceService

  Node = Struct.new(:host, :port, :cloud, :region, :zone, :public_ip, :count, :is_down, :down_since)
  @@mutex = Concurrent::ReentrantReadWriteLock.new
  @@last_refresh_time = -1
  @@control_connection = nil
  @@cluster_info = { }

  def self.get_all_eligible_hosts(load_balancer)
    # Implement this method
  end

  def self.get_load(host)
    if @@cluster_info[host]
      @@cluster_info[host].count
    else
      0
    end
  end

  def self.decrement_connection_count(host)
    # log_msg "decrement_connection_count -------------- for #{host}"
    @@mutex.acquire_write_lock
    begin
      info = @@cluster_info[host]
      unless info.nil?
        info.count -= 1
        puts "DEBUG Decremented connection count for #{host} by one. Latest count: #{info.count}"
        if info.count < 0
          # Can go negative if we are here because of a connection that was created in a non-LB fashion
          info.count = 0
          puts "DEBUG Resetting connection count for #{host} to zero."
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
          log_msg("control conn #{@@control_connection} on #{@@control_connection.host}")
          begin
            refresh_yb_servers(lb_props.failed_host_reconnect_delay, @@control_connection)
            refresh_done = true
            log_msg("Refreshed info from yb_servers(): #{@@cluster_info}")
          rescue => err
            log_msg "Failed to refresh yb_servers() info with control connection on #{@@control_connection.host} - #{err}, trying with new control connection"
            if iopts[:host] == @@control_connection.host
              if @@cluster_info[iopts[:host]]
                @@cluster_info[iopts[:host]].is_down = true
                @@cluster_info[iopts[:host]].down_since = Time.now.to_i
              end

              log_msg "cluster info: " + @@cluster_info.to_s
              new_list = @@cluster_info.select {|k, v| !v.is_down }
              if new_list.length > 0
                h = new_list.keys.first
                iopts[:port] = new_list[h].port
                iopts[:host] = h
                log_msg "new selected node: #{h} and updated iopts = #{iopts}"
              else
                # return nil
                raise(PG::Error, "Unable to create a control connection")
              end
            end
            log_msg "retrying control connection with iopts: " + iopts.to_s
            @@control_connection = create_control_connection(iopts)
            log_msg("control conn #{@@control_connection} created on #{@@control_connection.host}")
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
        log_msg "No hosts available"
        break
      end
      lb_host = host_port[0]
      lb_port = host_port[1]
      placement_index = host_port[2]
      if lb_host.empty?
        log_msg "No hosts available"
        break
      end
      # modify iopts args
      begin
        iopts[:host] = lb_host
        iopts[:port] = lb_port
        # iopts = resolve_hosts(iopts)
        log_msg("iopts before creating a connection: " + iopts.to_s + ", iopts class: " + iopts.class.to_s)
        connection = PG.connect(iopts)
        success = true
        log_msg("user connection: #{connection} on #{connection.host}")
      rescue => e
        @@mutex.acquire_write_lock
        begin
          @@cluster_info[lb_host].is_down = true
          @@cluster_info[lb_host].down_since = Time.now.to_i
          @@cluster_info[lb_host].count -= 1
          if @@cluster_info[lb_host].count < 0
            @@cluster_info[lb_host].count = 0
            log_msg("DEBUG Negative count was reset to zero for #{lb_host}")
          end
        ensure
          @@mutex.release_write_lock
        end
        log_msg("Connection creation failed: #{e}")
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
        log_msg("Attempting control connection ... #{iopts}")
        conn = PG.connect(iopts)
        # log_msg "connection members = " + conn.instance_variables.to_s
        # log_msg "connection methods = " + (conn.methods - Object.methods).to_s
        success = true
        log_msg("Returning control connection: #{conn}")
      rescue => e
        log_msg "Creating control connection failed with #{e}"
        # @@cluster_info[]
        if @@cluster_info[iopts[:host]]
          @@cluster_info[iopts[:host]].is_down = true
          @@cluster_info[iopts[:host]].down_since = Time.now.to_i
        end

        log_msg "retrying with cluster info: " + @@cluster_info.to_s
        new_list = @@cluster_info.select {|k, v| !v.is_down }
        if new_list.length > 0
          h = new_list.keys.first
          iopts[:port] = new_list[h].port
          iopts[:host] = h
          log_msg "new selected node: #{h} and updated iopts = #{iopts}"
        else
          raise(PG::Error, "Unable to create a control connection")
        end
      end
    end
    conn
  end

  def self.log_msg(msg)
    puts "-----> " + msg
  end

  def self.refresh_yb_servers(failed_host_reconnect_delay_secs, conn)
    log_msg("Refreshing yb_servers() on #{conn}")  #", methods: #{conn.methods.sort}"
    # conninfo, host, close, lo_close, loclose
    rs = conn.exec("select * from yb_servers()")
    rs.each do |row|
      # Take the first address of resolved host addresses
      host = resolve_host(row['host'])[0][0] # 2D array
      port = row['port']
      cloud = row['cloud']
      region = row['region']
      zone = row['zone']
      public_ip = row['public_ip']
      public_ip = resolve_host(public_ip) if public_ip

      # todo set useHostColumn field
      log_msg("refreshing host: #{host} ...")
      old = @@cluster_info[host]
      if old
        log_msg("host entry already present")
        if old.is_down
          if Time.now.to_i - old.down_since > failed_host_reconnect_delay_secs
            old.is_down = false
          else
            log_msg("host entry already present, but recently marked as down")
          end
          @@cluster_info[host] = old
        end
      else
        log_msg("creating new host entry ...")
        node = Node.new(host, port, cloud, region, zone, public_ip, 0, false, 0)
        @@cluster_info[host] = node
      end
    end
    @@last_refresh_time = Time.now.to_i
    log_msg("cluster_info: " + @@cluster_info.to_s)
  end

  def self.get_least_loaded_server(allowed_placements, fallback_to_tk_only, new_request, placement_index)
    current_index = 1
    selected = Array.new
    unless allowed_placements.nil? # topology-aware
      log_msg "topology keys given #{allowed_placements}, new? #{new_request}, placement index #{placement_index}"
      eligible_hosts = Array.new
      (placement_index..10).each { |idx|
        current_index = idx
        selected.clear
        min_connections = 1000000 # Using some really high value
        @@cluster_info.each do |host, node_info|
          unless node_info.is_down
            unless allowed_placements[idx].nil?
              allowed_placements[idx].each do |cp|
                log_msg "checking #{host} with cp: #{cp[0]}, #{cp[1]}, #{cp[2]}"
                if cp[0] == node_info.cloud && cp[1] == node_info.region && (cp[2] == node_info.zone || cp[2] == "*")
                  log_msg "eligible host #{host}"
                  eligible_hosts << host
                  if node_info.count < min_connections
                    min_connections = node_info.count
                    selected.clear
                    log_msg "Found new least loaded host: #{host} - #{node_info.count}"
                    selected.push(host)
                  elsif node_info.count == min_connections
                    log_msg "Found another least loaded host: #{host} - #{node_info.count}"
                    selected.push(host)
                  end
                  break # Move to the next node
                end
              end
            end
          end
        end
        log_msg "Found #{selected.length} hosts in level #{idx} with least load"
        if selected.length > 0
          break
        end
      }
    end

    if allowed_placements.nil? || (selected.empty? && !fallback_to_tk_only) # cluster-aware || fallback_to_tk_only = false
      unless allowed_placements.nil?
        log_msg "Falling back to rest of the cluster"
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
      log_msg "returning nil from get_least_loaded_server()"
      nil
    else
      index = rand(selected.size)
      selected_node = selected[index]
      log_msg("least loaded host: #{selected_node}")
      @@cluster_info[selected_node].count += 1
      Array[selected_node, @@cluster_info[selected_node].port, current_index]
    end
  end

  def self.metadata_needs_refresh(refresh_interval)
    log_msg("now: #{Time.now.to_i},  last refresh time: #{@@last_refresh_time}")
    if Time.now.to_i - @@last_refresh_time >= refresh_interval # || force_refresh == true
      log_msg("Time to refresh yb_servers()")
      true
    else
      log_msg("No refresh of yb_servers() needed")
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
