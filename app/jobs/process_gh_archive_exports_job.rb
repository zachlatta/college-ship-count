class ProcessGhArchiveExportsJob < ApplicationJob
  queue_as :default

  DISABLE_ACTIVE_RECORD_LOGGING = true
  BATCH_SIZE = 10_000

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

      # must be processed first, as it is used by the other models
      process_users_in_file(file)

      # Process repos and emails in parallel
      process_repos_in_file(file)
      process_emails_in_file(file)
    end

    # Turn logging back on if it was disabled
    ActiveRecord::Base.logger = original_logger if DISABLE_ACTIVE_RECORD_LOGGING

    files
  end

  private

  def process_users_in_file(file)
    Rails.logger.info "Processing users in file: #{file}"
    batch_process_upsert(file, GhArchive::User) do |events, batch|
      events.each do |e|
        next unless e[:actor]
        user_id = e[:actor][:id]
        batch[user_id] = {
          username: e[:actor][:login],
          id: user_id
        }
      end
    end
  end

  def process_repos_in_file(file)
    Rails.logger.info "Processing repos in file: #{file}"
    batch_process_upsert(file, GhArchive::Repo) do |events, batch|
      # Extract unique owner usernames from events
      owner_usernames = events.map do |e| 
        e[:repo][:name].split('/').first if e[:repo]
      end.compact.uniq
      
      # Batch lookup users by username
      users_by_username = GhArchive::User.select(:username, :id)
                                       .where(username: owner_usernames)
                                       .index_by(&:username)
      
      events.each do |e|
        next unless e[:repo]
        repo_id = e[:repo][:id]
        
        # Split repo name into owner and repo parts
        owner, repo_name = e[:repo][:name].split('/')
        next unless owner && repo_name # Skip if name doesn't have expected format
        
        # Get user from preloaded hash
        user = users_by_username[owner]
        next unless user # Skip if we can't find the user
        
        batch[repo_id] = {
          id: repo_id,
          name: repo_name,
          gh_archive_user_id: user.id
        }
      end
    end
  end

  def process_emails_in_file(file)
    Rails.logger.info "Processing emails in file: #{file}"
    batch_process_upsert(
      file, 
      GhArchive::KnownEmail,
      unique_by: [:gh_archive_user_id, :email],
      update_only: [:name, :is_private_email]
    ) do |events, batch|
      events.each do |e|
        next unless e[:type] == "PushEvent"
        user_id = e[:actor][:id]
        
        commits = e[:payload][:commits]
        commits&.each do |commit|
          author = commit[:author]
          next unless author && author[:email].present?

          email = author[:email]
          name = author[:name]

          # Use composite key for tracking latest entry
          key = "#{user_id}-#{email}"
          batch[key] = {
            email: email,
            name: name,
            is_private_email: GhArchive::KnownEmail.private_email?(email),
            gh_archive_user_id: user_id
          }
        end
      end
    end
  end

  def batch_process_upsert(file, model, upsert_options = {})
    batch = {}
    current_events = []

    read_json_events_archive(file) do |event|
      current_events << event
      
      if current_events.size >= BATCH_SIZE
        yield(current_events, batch)
        model.upsert_all(batch.values, **upsert_options)
        batch = {}
        current_events = []
      end
    end

    # Process any remaining events
    yield(current_events, batch) if current_events.any?
    model.upsert_all(batch.values, **upsert_options) if batch.any?
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
