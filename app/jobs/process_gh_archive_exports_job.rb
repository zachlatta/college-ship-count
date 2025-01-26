class ProcessGhArchiveExportsJob < ApplicationJob
  queue_as :default

  DISABLE_ACTIVE_RECORD_LOGGING = true

  def perform(*args)
    files = Dir.glob("external_storage/gharchive.org/*")
    Rails.logger.info "Found #{files.count} files"

    if DISABLE_ACTIVE_RECORD_LOGGING
      Rails.logger.info "DISABLE_ACTIVE_RECORD_LOGGING is true, disabling ActiveRecord logging"
      original_logger = ActiveRecord::Base.logger
      ActiveRecord::Base.logger = nil
    end

    files.each do |file|
      Rails.logger.info "Processing file: #{file}"

      read_json_events_archive(file) do |e|
        pp e
        GhArchive::User.upsert(
          {
            username: e[:actor][:login],
            id: e[:actor][:id]
          }
        )
      end
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
