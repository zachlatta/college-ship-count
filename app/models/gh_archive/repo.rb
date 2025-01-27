module GhArchive
  class Repo < ApplicationRecord
    belongs_to :owner, class_name: 'GhArchive::User', foreign_key: :gh_archive_user_id

    validates :name, presence: true
    validates :owner, presence: true
  end
end
