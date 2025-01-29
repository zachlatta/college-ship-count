require 'open-uri'

class ProcessGhArchiveExportsJob < ApplicationJob
  queue_as :default

  DISABLE_ACTIVE_RECORD_LOGGING = true
  BATCH_SIZE = 10_000.freeze
  GH_ARCHIVE_BASE_URL = "https://data.gharchive.org".freeze

  START_DATE = Date.new(2024, 1, 1).freeze
  END_DATE = Date.new(2024, 12, 31).freeze

  def perform(start_date = START_DATE, end_date = END_DATE)
    validate_dates(start_date, end_date)
    
    Rails.logger.info "Processing GH Archive data from #{start_date} to #{end_date}"

    if DISABLE_ACTIVE_RECORD_LOGGING
      Rails.logger.info "DISABLE_ACTIVE_RECORD_LOGGING is true, disabling ActiveRecord logging"
      original_logger = ActiveRecord::Base.logger
      ActiveRecord::Base.logger = nil
    end

    # Create a unique temp directory for this job run
    Dir.mktmpdir("gharchive-", Rails.root.join('tmp')) do |temp_dir|
      generate_download_urls(start_date, end_date).each do |url|
        filename = File.basename(url)
        temp_file = File.join(temp_dir, filename)
        
        begin
          download_file(url, temp_file)
          process_downloaded_file(temp_file)
        ensure
          FileUtils.rm_f(temp_file) if File.exist?(temp_file)
        end
      end
    end

  ensure
    ActiveRecord::Base.logger = original_logger if defined?(original_logger) && DISABLE_ACTIVE_RECORD_LOGGING
  end

  private

  def validate_dates(start_date, end_date)
    raise ArgumentError, "start_date must be before end_date" if start_date > end_date
    raise ArgumentError, "dates cannot be in the future" if end_date > Date.current
  end

  def generate_download_urls(start_date, end_date)
    urls = []
    (start_date..end_date).each do |date|
      24.times do |hour|
        urls << "#{GH_ARCHIVE_BASE_URL}/#{date.strftime('%Y-%m-%d')}-#{hour}.json.gz"
      end
    end
    urls
  end

  def download_file(url, destination)
    Rails.logger.info "Downloading #{url}"
    
    URI.open(url) do |remote_file|
      File.open(destination, 'wb') do |local_file|
        local_file.write(remote_file.read)
      end
    end
  rescue OpenURI::HTTPError => e
    if e.io.status[0] == "404"
      Rails.logger.warn "File not found: #{url}"
    else
      raise
    end
  end

  def process_downloaded_file(file_path)
    return unless File.exist?(file_path)
    
    Rails.logger.info "Processing file: #{file_path}"
    
    # must be processed first, as it is used by the other models
    process_users_in_file(file_path)
    process_repos_in_file(file_path)
    process_emails_in_file(file_path)
  end

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

  def read_json_events_archive(file)
    return unless File.exist?(file)
    
    Zlib::GzipReader.open(file) do |gz|
      while (line = gz.gets)
        yield JSON.parse(line, symbolize_names: true)
      end
    end
  end
end
