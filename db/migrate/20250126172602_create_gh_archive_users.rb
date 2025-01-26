class CreateGhArchiveUsers < ActiveRecord::Migration[8.0]
  def change
    create_table :gh_archive_users, id: false do |t|
      t.bigint :id, primary_key: true
      t.string :username, null: false

      t.timestamps
    end

    add_index :gh_archive_users, :username
  end
end
