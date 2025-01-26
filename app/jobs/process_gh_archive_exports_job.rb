class ProcessGhArchiveExportsJob < ApplicationJob
  queue_as :default

  DISABLE_ACTIVE_RECORD_LOGGING = true
  BATCH_SIZE = 5000

  def perform(*args)
    # files named like YYY-MM-DD-h.json.gz, sorted by date and hour
    files = sorted_gh_archive_filepaths

    Rails.logger.info "Found #{files.count} files"

    if DISABLE_ACTIVE_RECORD_LOGGING
      Rails.logger.info "DISABLE_ACTIVE_RECORD_LOGGING is true, disabling ActiveRecord logging"
      original_logger = ActiveRecord::Base.logger
      ActiveRecord::Base.logger = nil
    end

    files.each do |file|
      Rails.logger.info "Processing file: #{file}"

      user_batch = {}
      known_email_batch = {}  # Changed to hash to track latest entries

      read_json_events_archive(file) do |e|
        user_id = e[:actor][:id]
        if e[:actor]
          user_batch[user_id] = {
            username: e[:actor][:login],
            id: user_id
          }
        end

        case e[:type]
        when "PushEvent"
          commits = e[:payload][:commits]
          commits&.each do |commit|
            author = commit[:author]
            next unless author && author[:email].present?

            email = author[:email]
            name = author[:name]

            # Use composite key for tracking latest entry
            key = "#{user_id}-#{email}"
            known_email_batch[key] = {
              email: email,
              name: name,
              is_private_email: GhArchive::KnownEmail.private_email?(email),
              gh_archive_user_id: user_id
            }
          end
        end
        
        if user_batch.size >= BATCH_SIZE
          GhArchive::User.upsert_all(user_batch.values)
          user_batch = {}
        end

        if known_email_batch.size >= BATCH_SIZE
          GhArchive::KnownEmail.upsert_all(
            known_email_batch.values,
            unique_by: [:gh_archive_user_id, :email],
            update_only: [:name, :is_private_email]
          )
          known_email_batch = {}
        end
      end

      # Insert any remaining records
      GhArchive::User.upsert_all(user_batch.values) if user_batch.any?
      GhArchive::KnownEmail.upsert_all(
        known_email_batch.values,
        unique_by: [:gh_archive_user_id, :email],
        update_only: [:name, :is_private_email]
      ) if known_email_batch.any?
    end

    # Turn logging back on if it was disabled
    ActiveRecord::Base.logger = original_logger if DISABLE_ACTIVE_RECORD_LOGGING

    files
  end

  def sorted_gh_archive_filepaths
    Dir.glob("external_storage/gharchive.org/*").sort_by do |file|
      # Extract date and hour from filename
      date_str = File.basename(file, '.json.gz')
      # Convert hour part to padded number for proper sorting
      date_str.sub(/-(\d{1,2})$/) { |m| "-%02d" % $1.to_i }
    end
  end

  def read_json_events_archive(file)
    Zlib::GzipReader.open(file) do |gz|
      while (line = gz.gets)
        yield JSON.parse(line, symbolize_names: true)
      end
    end
  end
end
