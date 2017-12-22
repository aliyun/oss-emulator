require 'emulator/logging'
require 'emulator/base'

module OssEmulator

  ##
  # Config support
  # @example
  #   require 'config'
  #   Config.init() 
  #   Config.set_store(store_root)
  #   Config.set_quiet_mode(false)
  class Config

    @@config_init = false
    @@quiet_mode = true

    def self.init()
      return if @@config_init

      @@store_root ||= Store::STORE_ROOT_DIR
      @@host       ||= HttpMsg::HOST
      @@hostnames  ||= HttpMsg::HOSTNAMES

      Log.init()
      @@config_init = true
    end

    def self.set_quiet_mode(mode)
      return unless @@config_init
      @@quiet_mode = mode
      Log.set_quiet_mode(mode)
    end

    def self.set_log_level(level)
      return unless @@config_init
      Log.set_log_level(level)
    end

    def self.set_store(store_root)
      return unless @@config_init
      @@store_root = store_root
    end

    def self.store()
      return unless @@config_init
      @@store_root
    end

    def self.set_hostname(host)
      return unless @@config_init
      @@host = host
      @@hostnames << host
    end

    def self.host()
      return unless @@config_init
      @@host
    end

    def self.hostnames()
      return unless @@config_init
      @@hostnames
    end

  end # Config

end # OssEmulator
