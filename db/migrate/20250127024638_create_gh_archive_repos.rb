class CreateGhArchiveRepos < ActiveRecord::Migration[8.0]
  def change
    create_table :gh_archive_repos, id: false do |t|
      t.bigint :id, primary_key: true
      t.string :name, null: false
      t.bigint :gh_archive_user_id, null: false

      t.timestamps
    end

    add_index :gh_archive_repos, :gh_archive_user_id
    add_index :gh_archive_repos, :name
  end
end
