require 'jruby'

module LogStash module PluginMixins module Jdbc
  module Common
    # XXX make these configurable
    # Each database has a wtf_init table
    WTF_INIT_TABLE_NAME = "wtf_init"
    WTF_INIT_TABLE_ENDPOINT_COL = "endpoint"
    WTF_LOCAL_MYSQL_SERVER_PORT = 3307

    private

    # NOTE: using the JRuby mechanism to load classes (through JavaSupport)
    # makes the lock redundant although it does not hurt to have it around.
    DRIVERS_LOADING_LOCK = java.util.concurrent.locks.ReentrantLock.new()

    def wtf_register
      # XXX require all plugins to impl wtf_register(), and call it in ls core after register()

      # Start a WTF MySql server, register it with the database
      # XXX when server get trace, call the trace handler here (need to pass this class to Java, or just impl the server here...)

      # XXX (later) support non-mysql jdbc dbs (choose server impl based on db used for data)
      # XXX (later) could consider moving the logic here and in the trace handler into the jdbc driver
      # (possibly using an interceptor)
      @mysql_server = @mysql_server_impl.new(WTF_LOCAL_MYSQL_SERVER_PORT)
      @mysql_server.start()
      @database.run("CREATE TABLE IF NOT EXISTS #{WTF_INIT_TABLE_NAME} (
        #{WTF_INIT_TABLE_ENDPOINT_COL} VARCHAR(2000) # TODO support bodies
        );")
      # XXX get local mysql server IP
      @database.run("INSERT INTO #{WTF_INIT_TABLE_NAME} (#{WTF_INIT_TABLE_ENDPOINT_COL}) VALUES (
        \"localhost:#{WTF_LOCAL_MYSQL_SERVER_PORT}\"
        );")
    end

    # Use Sequel to connect to upstream WTF mysql servers
    def wtf_handle_trace
      @logger.info("ZZEM wtf_handle_trace; pipeline #{execution_context.pipeline_id()}")
      scheme = @jdbc_connection_string.split("//")[0]
      @database["SELECT #{WTF_INIT_TABLE_ENDPOINT_COL.to_sym} FROM #{WTF_INIT_TABLE_NAME.to_sym}"].all do |wtf_endpoint|
        wtf_conn_string = scheme + "//" + wtf_endpoint[WTF_INIT_TABLE_ENDPOINT_COL.to_sym] + "?useSSL=false"
        begin
          upstream_wtf_mysql_server = Sequel.connect(wtf_conn_string)
        rescue => e
          @logger.warn("WTF trace handler failed to connect to upstream #{wtf_conn_string}", :exception => e.message)
        end
      end
    end

    def complete_sequel_opts(defaults = {})
      sequel_opts = @sequel_opts.
          map { |key,val| [key.is_a?(String) ? key.to_sym : key, val] }.
          map { |key,val| [key, val.eql?('true') ? true : (val.eql?('false') ? false : val)] }
      sequel_opts = defaults.merge Hash[sequel_opts]
      sequel_opts[:user] = @jdbc_user unless @jdbc_user.nil? || @jdbc_user.empty?
      sequel_opts[:password] = @jdbc_password.value unless @jdbc_password.nil?
      sequel_opts[:driver] = @driver_impl # Sequel uses this as a fallback, if given URI doesn't auto-load the driver correctly
      sequel_opts
    end

    def load_driver
      return @driver_impl if @driver_impl ||= nil

      require "java"
      require "sequel"
      require "sequel/adapters/jdbc"

      # execute all the driver loading related duties in a serial fashion to avoid
      # concurrency related problems with multiple pipelines and multiple drivers
      DRIVERS_LOADING_LOCK.lock()
      begin
        load_driver_jars
        begin
          @driver_impl = load_class(@jdbc_driver_class)
          # TODO make class name configurable
          @mysql_server_impl = load_class("com.mysql.cj.jdbc.MysqlServer")

        rescue => e # catch java.lang.ClassNotFoundException, potential errors
          # (e.g. ExceptionInInitializerError or LinkageError) won't get caught
          message = if jdbc_driver_library_set?
                      "Are you sure you've included the correct jdbc driver in :jdbc_driver_library?"
                    else
                      ":jdbc_driver_library is not set, are you sure you included " +
                          "the proper driver client libraries in your classpath?"
                    end
          raise LogStash::PluginLoadingError, "#{e.inspect}. #{message}"
        end
      ensure
        DRIVERS_LOADING_LOCK.unlock()
      end
    end

    def load_driver_jars
      if jdbc_driver_library_set?
        @jdbc_driver_library.split(",").each do |driver_jar|
          @logger.debug("loading #{driver_jar}")
          # load 'driver.jar' is different than load 'some.rb' as it only causes the file to be added to
          # JRuby's class-loader lookup (class) path - won't raise a LoadError when file is not readable
          unless FileTest.readable?(driver_jar)
            raise LogStash::PluginLoadingError, "unable to load #{driver_jar} from :jdbc_driver_library, " +
                "file not readable (please check user and group permissions for the path)"
          end
          begin
            require driver_jar
          rescue LoadError => e
            raise LogStash::PluginLoadingError, "unable to load #{driver_jar} from :jdbc_driver_library, #{e.message}"
          rescue StandardError => e
            raise LogStash::PluginLoadingError, "unable to load #{driver_jar} from :jdbc_driver_library, #{e}"
          end
        end
      end
    end

    def jdbc_driver_library_set?
      !@jdbc_driver_library.nil? && !@jdbc_driver_library.empty?
    end

    # Class and all its transitive deps should be in the driver jar
    def load_class(class_name)
      # sub a potential: 'Java::org::my.Driver' to 'org.my.Driver'
      klass = class_name.gsub('::', '.').sub(/^Java\./, '')
      # NOTE: JRuby's Java::JavaClass.for_name which considers the custom class-loader(s)
      # in 9.3 the API changed and thus to avoid surprises we go down to the Java API :
      klass = JRuby.runtime.getJavaSupport.loadJavaClass(klass) # throws ClassNotFoundException
      # unfortunately we can not simply return the wrapped java.lang.Class instance as
      # Sequel assumes to be able to do a `driver_class.new` which only works on the proxy,
      org.jruby.javasupport.Java.getProxyClass(JRuby.runtime, klass)
    end

  end
end end end
