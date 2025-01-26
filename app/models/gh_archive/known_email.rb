class GhArchive::KnownEmail < ApplicationRecord
  belongs_to :gh_archive_user

  validates :email, presence: true, 
                   format: { with: URI::MailTo::EMAIL_REGEXP },
                   uniqueness: { scope: :gh_archive_user_id }
  validates :is_private_email, inclusion: { in: [true, false] }
  
  scope :public_emails, -> { where(is_private_email: false) }
  scope :private_emails, -> { where(is_private_email: true) }

  before_validation :set_private_email_status

  def self.private_email?(email)
    email&.end_with?('users.noreply.github.com')
  end

  private

  def set_private_email_status
    self.is_private_email = self.class.private_email?(email)
  end
end
