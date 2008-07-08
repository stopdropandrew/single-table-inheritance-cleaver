ActiveRecord::Schema.define(:version => 0) do
  create_table :high_scores, :force => true do |t|
    t.string :type
    t.date :date
    t.integer :user_id, :value, :statistic_id
  end

  create_table :daily_high_scores, :force => true do |t|
    t.date :date
    t.integer :user_id, :value, :statistic_id
  end

  create_table :weekly_high_scores, :force => true do |t|
    t.date :date
    t.integer :user_id, :value, :statistic_id
  end

  create_table :lifetime_high_scores, :force => true do |t|
    t.integer :user_id, :value, :statistic_id
  end
  
  create_table :custom_high_scores, :force => true do |t|
    t.date :date
    t.integer :user_id, :value, :statistic_id
  end 
end
