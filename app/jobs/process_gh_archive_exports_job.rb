class ProcessGhArchiveExportsJob < ApplicationJob
  queue_as :default

  DISABLE_ACTIVE_RECORD_LOGGING = true

  def perform(*args)
    # files named like YYY-MM-DD-h.json.gz, sorted by date and hour
    files = Dir.glob("external_storage/gharchive.org/*").sort_by do |file|
      # Extract date and hour from filename
      date_str = File.basename(file, '.json.gz')
      # Convert hour part to padded number for proper sorting
      date_str.sub(/-(\d{1,2})$/) { |m| "-%02d" % $1.to_i }
    end

    Rails.logger.info "Found #{files.count} files"

    if DISABLE_ACTIVE_RECORD_LOGGING
      Rails.logger.info "DISABLE_ACTIVE_RECORD_LOGGING is true, disabling ActiveRecord logging"
      original_logger = ActiveRecord::Base.logger
      ActiveRecord::Base.logger = nil
    end

    files.each do |file|
      Rails.logger.info "Processing file: #{file}"
      batch = {}
      
      read_json_events_archive(file) do |e|
        user_id = e[:actor][:id]
        batch[user_id] = {
          username: e[:actor][:login],
          id: user_id
        }
        
        if batch.size >= 1000
          GhArchive::User.upsert_all(batch.values)
          batch = {}
        end
      end

      # Insert any remaining records
      GhArchive::User.upsert_all(batch.values) if batch.any?
    end

    # Turn logging back on if it was disabled
    ActiveRecord::Base.logger = original_logger if DISABLE_ACTIVE_RECORD_LOGGING

    files
  end

  def read_json_events_archive(file)
    Zlib::GzipReader.open(file) do |gz|
      while (line = gz.gets)
        yield JSON.parse(line, symbolize_names: true)
      end
    end
  end
end
