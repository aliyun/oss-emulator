require 'thor'
require 'emulator/server'
require 'emulator/config'

module OssEmulator
  class CommandLine < Thor
    default_task("server")

    desc "server", "Run a server on a particular hostname"
    method_option :root, :type => :string, :aliases => '-r', :required => true, :desc => "The root directory to store bucket/object files."
    method_option :port, :type => :numeric, :aliases => '-p', :default => 80, :required => true, :desc => "Bind to this port. Defaults to 80 port."
    method_option :address, :type => :string, :aliases => '-a', :required => false, :desc => "Bind to this address. Defaults to all IP addresses of the machine."
    method_option :hostname, :type => :string, :aliases => '-H', :desc => "The root name of the host. "
    method_option :quiet, :type => :boolean, :aliases => '-q', :default => true, :desc => "Quiet; do not write anything to standard output."
    method_option :loglevel, :type => :string, :aliases => '-L', :desc => "Set the log level : fatal、error、warn、info、debug. "
    method_option :sslcert, :type => :string, :desc => "Path to SSL certificate"
    method_option :sslkey, :type => :string, :desc => "Path to SSL certificate key"

    def server
      Config.init()
      Config.set_quiet_mode(options[:quiet])
      Config.set_log_level(options[:loglevel].downcase) if options[:loglevel]
      
      if options[:root]
        root_dir = File.expand_path(options[:root])
        if root_dir==File.expand_path(Store::STORE_ROOT_DIR) && !File.exist?(root_dir)
          FileUtils.mkdir_p(root_dir)
        end

        Log.abort("The root directory does not exist : #{root_dir}") unless File.exist?(root_dir)
      end
      Config.set_store(root_dir)

      hostname = 'oss.aliyun.com'
      if options[:hostname]
        hostname = options[:hostname]

        # In case the user has put a port on the hostname
        if hostname =~ /:(\d+)/
          hostname = hostname.split(":")[0]
        end
      end
      Config.set_hostname(hostname)

      address = options[:address]
      ssl_cert_path = options[:sslcert]
      ssl_key_path = options[:sslkey]

      if (ssl_cert_path.nil? && !ssl_key_path.nil?) || (!ssl_cert_path.nil? && ssl_key_path.nil?)
        Log.abort("If you specify an SSL certificate you must also specify an SSL certificate key")
      end
  
      # Start Server
      Log.info("Loading OssEmulator on port #{options[:port]} with hostname #{Config.host} . ")
      Log.info("OssEmulator Store root is #{Config.store}, Log level is #{Log.level} . ")
      server = OssEmulator::Server.new(address, options[:port], hostname, ssl_cert_path, ssl_key_path, quiet: !!options[:quiet])
      server.serve
    end

    desc "version", "Report the current OSS Emulator version"
    def version
      puts Version::VERSION_STRING
    end

  end
end
