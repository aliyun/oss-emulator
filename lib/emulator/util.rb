require 'yaml'
require 'find'
require 'time'
require 'fileutils'
require 'builder'
require "thread"
require 'emulator/config'

module OssEmulator
  module OssUtil

    def self.generate_request_id()
      n = 24
      o = [('0'..'9'),('A'..'F')].map{|i| i.to_a}.flatten
      (0...n).map{ o[rand(o.length)] }.join
    end

    def self.valid_bucket_name(name)
      return false unless name.is_a?(String)
  
      set = Bucket::BUCKET_NAME_CHAR_SET
      sub_set = set[0..-2]
      if not (name.length>=3 && name.length<=Bucket::MAX_BUCKET_NAME_LENGTH && sub_set.include?(name[0]) && sub_set.include?(name[-1]))
        return false
      end

      name.split(//).each do |a|
        if !set.include?(a)
          return false
        end
      end

      return true
    end

    def self.valid_object_name(name)
      if !name.is_a?(String)
        return false
      end

      if name.encoding!=Encoding::UTF_8
        return false
      end
  
      if name[0]=='\\' || name[0]=='/'
        return false
      end
      
      if name.bytesize<1 || name.bytesize>Object::MAX_OBJECT_NAME_LENGTH
        return false
      end

      slice_list = name.split('/')
      slice_list.each do |slice|
        return false if slice.bytesize>Object::MAX_OBJECT_NAME_SLICE_LENGTH
      end

      return true
    end

    def self.get_bucket_list_objects(lbr, req)
      marker = req.query["marker"] ? req.query["marker"].to_s : nil
      marker_found = (marker==nil || marker=="") ? true : false
      prefix = req.query["prefix"] ? req.query["prefix"].to_s : nil
      prefix = nil if prefix==""
      max_keys = req.query["max-keys"] ? req.query["max-keys"].to_i : 100
      max_keys = max_keys>1000 ? 1000 : max_keys
      delimiter = req.query["delimiter"] ? req.query["delimiter"].to_s : nil
      delimiter = nil if delimiter==""

      if delimiter
        if prefix
          base_prefix = prefix
        else
          base_prefix = ""
        end
        prefix_offset = base_prefix.length
      end

      bucket_path = File.join(Config.store, req.bucket, '/')
      find_root_folder = bucket_path 
      object_list = []
      common_prefix_list = []
      is_truncated = false
      count = 0

      Find.find(find_root_folder) do |filename|
        if File.basename(filename)==Store::OBJECT_METADATA
          key_name = File.dirname(filename).gsub(bucket_path, "")
          if marker_found && (!prefix || key_name.index(prefix)==0 || key_name.index(prefix)==1)
            if delimiter
              right_key_name = key_name.slice(prefix_offset, key_name.length)
              right_parts = right_key_name.split(delimiter, 2)
              
              if right_parts.length>1
                mid_part = right_parts[0]
                common_prefix = prefix + mid_part + delimiter
                if !common_prefix_list.include?(common_prefix)
                  count += 1
                  if count <= max_keys
                    common_prefix_list << common_prefix
                  else
                    is_truncated = true
                    break
                  end
                end

                next
              end

            end
            
            count += 1
            if count <= max_keys
              obj_hash = {}
              obj_hash[:bucket] = req.bucket
              obj_hash[:key] = key_name
              metadata = File.open(filename) { |file| YAML::load(file) }
              obj_hash[:md5] = metadata.key?(:md5) ? metadata[:md5].upcase : ''
              obj_hash[:content_type] = metadata.fetch(:content_type) { "application/octet-stream" }
              obj_hash[:size] = metadata.fetch(:size) { 0 }
              obj_hash[:storageclass] = "Standard"
              obj_hash[:modified_date] = metadata[:modified_date]
              object_list << obj_hash
            else
              is_truncated = true
              break
            end
          end

          cmp = marker<=>key_name
          if marker && (cmp <= 0)
            marker_found = true
          end
        end # if 
      end # Find.find

      lbr.IsTruncated(is_truncated)
      object_list.each do |obj_hash|
        lbr.Contents { |contents|
          contents.Key(obj_hash[:key])
          contents.LastModified(Time.parse(obj_hash[:modified_date]).strftime('%Y-%m-%dT%H:%M:%S.000Z'))
          contents.ETag(obj_hash[:md5])
          contents.Type("Multipart")
          contents.Size(obj_hash[:size])
          contents.StorageClass(obj_hash[:storageclass])
          contents.Owner { |node| 
            node.ID("00220120222")
            node.DisplayName("1390402650033793")
          }
        }
      end
      
      common_prefix_list.each do |item|
        lbr.CommonPrefixes { |node|
          node.Prefix(item)
        }
      end
 
    end #get_bucket_list_objects
    
    def self.delete_object_file_and_dir(bucket, object)
      Log.debug("delete_object_file_and_dir", 'yellow')
      bucket_dir = File.join(Config.store, bucket)
      object_dir = File.join(bucket_dir, object)

      object_metadata_filename = File.join(object_dir, Store::OBJECT_METADATA)
      FileUtils.rm_rf(object_metadata_filename) if File.exist?(object_metadata_filename)
      object_content_filename = File.join(object_dir, Store::OBJECT_CONTENT_PREFIX)
      FileUtils.rm_rf("#{object_content_filename}*") if File.exist?(object_content_filename)

      current_level_folder = object_dir
      while File.exist?(current_level_folder)
        return if current_level_folder==bucket_dir
        Find.find(current_level_folder) do |filename|
          Log.debug("filename", 'blue')
          return if filename.include?(Store::OBJECT_METADATA)
        end
        
        FileUtils.rm_rf(current_level_folder)
        current_level_folder = File.dirname(current_level_folder)
      end 
    end

    def self.put_object_metadata(bucket, object, request, options = nil)
      obj_dir = File.join(Config.store, bucket, object)

      temp_obj_dir = obj_dir
      temp_subdir = ''
      if options!=nil && options.include?(:temp_dir)
        temp_subdir = options[:temp_dir] if options.include?(:temp_dir)
        temp_obj_dir = File.join(obj_dir, temp_subdir)
        Log.debug("obj_dir=#{obj_dir}, options=#{options}, temp_subdir=#{temp_subdir}")
      end

      content_filename = File.join(temp_obj_dir, Store::OBJECT_CONTENT)

      Log.raise("put_object_metadata : Object content file does not exist in put_object_metadata : #{temp_obj_dir}") unless File.exist?(content_filename)

      # construct metadata
      metadata = {}
      metadata[:bucket] = bucket
      metadata[:object] = object
      metadata[:acl] = "default"

      metadata[:creation_date] = Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
      metadata[:modified_date] = Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION)

      metadata[:content_type] = request.header["content-type"].first
      metadata[:content_disposition] = request.header['content-disposition'].first if request.header['content-disposition']
      metadata[:content_encoding] = request.header["content-encoding"].first

      if options
        metadata[:size] = options[:size] if options.include?(:size)
        metadata[:part_size] = options[:part_size] if options.include?(:part_size)
        metadata[:appendable] = options[:appendable] if options.include?(:appendable)
        metadata[:md5] = options.include?(:md5) ? options[:md5] : ''
      else
        metadata[:size] = File.size(content_filename)
        metadata[:part_size] = 0
        metadata[:md5] = Digest::MD5.file(content_filename).hexdigest 
      end

      # construct metadata : add custom metadata from the request header
      metadata[:oss_metadata] = {}
      metadata[:custom_metadata] = {}
      request.header.each do |key, value|
        match = /^x-oss-([^-]+)-(.*)$/.match(key)
        next unless match
        if match[1].eql?('meta') && (match_key = match[2])
          metadata[:custom_metadata][match_key] = value.join(', ')
          next
        end
        metadata[:oss_metadata][key.gsub(/^x-oss-/, '')] = value.join(', ')
      end

      # store metadata to file
      metadata_file = File.join(temp_obj_dir, Store::OBJECT_METADATA)
      File.open(metadata_file, 'w') do |f|
        f << YAML::dump(metadata)
      end

      # define the dynamic metux var 
      if temp_subdir!=''
        # bucket, object, str_var_metux = "@mutex_#{temp_subdir}"
        bucket_var = bucket.gsub(/\W/, '_')
        object_var = object.gsub(/\W/, '_')
        str_var_metux = "@mutex_#{bucket_var}_#{object_var}"
        instance_variable_set(str_var_metux, Mutex.new)
        Log.debug(instance_variable_get(str_var_metux), 'blue')
        # metux lock
        instance_variable_get(str_var_metux).lock

        # remove old object files
        list_oldfiles = Dir.entries(obj_dir) 
        list_oldfiles.each do |f|
          if f.include?("_object_oss_aliyun_ALIBABA")
            filepath = "#{obj_dir}/#{f}"
            Log.debug(filepath, "yellow")
            FileUtils.rm_rf(filepath)
          end
        end

        # move object files from temp_dir to normal dir
        Log.info("move temp_object to normal object. ")
        list_move = Dir.entries(temp_obj_dir) 
        list_move.each do |f| 
          FileUtils.mv "#{temp_obj_dir}/#{f}",obj_dir if !File.directory?(f) 
        end
        FileUtils.rm_rf(temp_obj_dir) if File.exist?(temp_obj_dir)

        # metux unlock
        instance_variable_get(str_var_metux).unlock
      end

      metadata
    end

  end # module OssUtil
end # module OssEmulator