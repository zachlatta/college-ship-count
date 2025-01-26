class CreateGhArchiveKnownEmails < ActiveRecord::Migration[8.0]
  def change
    create_table :gh_archive_known_emails do |t|
      t.references :gh_archive_user, null: false, foreign_key: true, index: true
      t.string :email, null: false
      t.string :name
      t.boolean :is_private_email, null: false, default: false

      t.timestamps
    end

    add_index :gh_archive_known_emails, :email
    add_index :gh_archive_known_emails, [:gh_archive_user_id, :email], unique: true, name: 'index_gh_archive_known_emails_on_user_and_email'
  end
end
