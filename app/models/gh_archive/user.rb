class GhArchive::User < ApplicationRecord
  validates :username, presence: true, uniqueness: true
  validates :id, presence: true, uniqueness: true
end
