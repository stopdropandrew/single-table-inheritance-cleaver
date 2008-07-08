require File.join(File.dirname(__FILE__), 'test_helper')

class SingleTableInheritanceCleaverTest < Test::Unit::TestCase
  def setup
    HighScore.delete_all
    DailyHighScore.delete_all
    WeeklyHighScore.delete_all
    LifetimeHighScore.delete_all
  end
  
  def test_cleaver_knows_what_the_table_will_be_split_into
    HighScore.create!(:type => 'DailyHighScore')
    HighScore.create!(:type => 'WeeklyHighScore')
    
    cleaver = SingleTableInheritanceCleaver.new(HighScore)
    assert_equal HighScore, cleaver.source
    assert_equal({'DailyHighScore' => 'daily_high_scores', 'WeeklyHighScore' => 'weekly_high_scores'}, cleaver.destinations)
  end
  
  def test_split_moves_data_to_correct_tables
    daily = HighScore.create!(:type => 'DailyHighScore', :value => 2)
    weekly = HighScore.create!(:type => 'WeeklyHighScore', :value => 3)
    
    cleaver = SingleTableInheritanceCleaver.new(HighScore)
    cleaver.cleave!
    
    assert_equal [2], DailyHighScore.find(:all).map(&:value)
    assert_equal [3], WeeklyHighScore.find(:all).map(&:value)
  end

  def test_cleaver_should_accept_chunk_size
    cleaver = SingleTableInheritanceCleaver.new(HighScore, :chunk_size => 10000)
    assert_equal 10000, cleaver.chunk_size, 'Should have used initializing chunk size'
  end
end
