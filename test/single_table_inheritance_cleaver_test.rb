require File.join(File.dirname(__FILE__), 'test_helper')

class SingleTableInheritanceCleaverTest < Test::Unit::TestCase
  
  def test_cleaver_knows_what_the_table_will_be_split_into
    HighScore.create!(:type => 'DailyHighScore')
    HighScore.create!(:type => 'WeeklyHighScore')
    
    cleaver = SingleTableInheritanceCleaver.new(HighScore)
    assert_equal HighScore, cleaver.source
    assert_equal({'DailyHighScore' => 'daily_high_scores', 'WeeklyHighScore' => 'weekly_high_scores'}, cleaver.destinations)
  end
  
  def test_specify_destination_table
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :destinations => { 'WeeklyHighScore' => 'custom_high_scores' })
    assert_equal({'WeeklyHighScore' => 'custom_high_scores'}, cleaver.destinations)
  end

  def test_specifying_destination_table_should_merge_with_default_destinations
    HighScore.create!(:type => 'DailyHighScore')
    HighScore.create!(:type => 'WeeklyHighScore')

    cleaver = SingleTableInheritanceCleaver.new(HighScore, :destinations => { 'WeeklyHighScore' => 'custom_high_scores' })
    assert_equal({'DailyHighScore' => 'daily_high_scores', 'WeeklyHighScore' => 'custom_high_scores'}, cleaver.destinations)
    # :conditions => { 'facebook_daily_high_scores' => ['statistic_id in ?', statistic_ids] }
  end

  def test_overriding_table_should_write_to_correct_table
    HighScore.create!(:type => 'WeeklyHighScore')

    cleaver = SingleTableInheritanceCleaver.new(HighScore, :destinations => { 'WeeklyHighScore' => 'custom_high_scores' })
    cleaver.cleave!
    assert_equal 1, CustomHighScore.count
    assert_equal 0, WeeklyHighScore.count
  end

  def test_adding_rejections_omits_data_for_specified_column
    HighScore.create!(:type => 'DailyHighScore', :user_id => 3)

    cleaver = SingleTableInheritanceCleaver.new(HighScore, :rejections => {'daily_high_scores' => 'user_id'})
    cleaver.cleave!
    
    assert_equal nil, DailyHighScore.find(:first).user_id
  end

  def test_adding_rejections_omits_data_for_multiple_columns
    HighScore.create!(:type => 'DailyHighScore', :user_id => 3, :statistic_id => 6)

    cleaver = SingleTableInheritanceCleaver.new(HighScore, :rejections => {'daily_high_scores' => ['user_id', 'statistic_id']})
    cleaver.cleave!

    assert_equal nil, DailyHighScore.find(:first).user_id
    assert_equal nil, DailyHighScore.find(:first).statistic_id
  end

  def test_cleave_moves_data_to_correct_tables_with_one_item_per_type
    daily = HighScore.create!(:type => 'DailyHighScore', :value => 2)
    weekly = HighScore.create!(:type => 'WeeklyHighScore', :value => 3)
    
    cleaver = SingleTableInheritanceCleaver.new(HighScore)
    cleaver.cleave!
    
    assert_equal [2], DailyHighScore.find(:all).map(&:value)
    assert_equal [3], WeeklyHighScore.find(:all).map(&:value)
  end

  def test_cleave_adds_correct_data_with_several_items_per_type
    generate_some_high_scores_to_cleave

    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => 7)
    cleaver.cleave!
    
    assert_equal [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], DailyHighScore.find(:all).map(&:value)
    assert_equal [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120], WeeklyHighScore.find(:all).map(&:value)
    
  end
  
  def test_cleaver_should_accept_chunk_size
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => 10000)
    assert_equal 10000, cleaver.chunk_size, 'Should have used initializing chunk size'
  end
  
  def test_cleaver_sets_default_chunk_size
    cleaver = SingleTableInheritanceCleaver.new(HighScore)
    assert_not_equal nil, cleaver.chunk_size, 'Should default the chunk size'
  end
  
  def test_cleave_chunk_adds_the_correct_number_of_rows
    generate_some_high_scores_to_cleave
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => 4)
    assert_difference DailyHighScore, :count, 4 do
      cleaver.cleave_chunk 'DailyHighScore', 'daily_high_scores'
    end
  end
  
  def test_cleave_chunk_only_cleaves_to_specified_destination
    generate_some_high_scores_to_cleave
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => HighScore.count) # ensure that the cleave size would capture the wrong type of records
    assert_difference WeeklyHighScore, :count, 0 do
      cleaver.cleave_chunk 'DailyHighScore', 'daily_high_scores'
    end
  end
  
  def test_cleave_chunk_returns_true_if_rows_added
    generate_some_high_scores_to_cleave
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => 3)
    assert cleaver.cleave_chunk('DailyHighScore', 'daily_high_scores')
  end
  
  def test_cleave_chunk_returns_nil_if_nothing_added
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => 3)
    assert !cleaver.cleave_chunk('DailyHighScore', 'daily_high_scores')
  end
  
  def test_cleave_chunk_adds_rows_at_specified_offset
    generate_some_high_scores_to_cleave
    records_to_move = 5
    offset = 6
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => records_to_move)
    cleaver.cleave_chunk('DailyHighScore', 'daily_high_scores', offset)
    
    expected_values = HighScore.find(:all, :conditions => {:type => 'DailyHighScore'}, :order => 'id', :offset => offset, :limit => records_to_move).map(&:value)
    assert_same_elements expected_values, DailyHighScore.find(:all).map(&:value)
  end
  
  def generate_some_high_scores_to_cleave
    (1..20).each do |i|
      HighScore.create!(:type => 'DailyHighScore', :value => i, :user_id => i, :statistic_id => 1)
      HighScore.create!(:type => 'WeeklyHighScore', :value => i + 100, :user_id => i, :statistic_id => 1)
    end
  end
end
