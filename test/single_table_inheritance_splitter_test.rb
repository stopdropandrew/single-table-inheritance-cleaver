require File.join(File.dirname(__FILE__), 'test_helper')

class SingleTableInheritanceSplitterTest < Test::Unit::TestCase
  def setup
  end
  
  def test_splitter_knows_what_the_table_will_be_split_into
    HighScore.create!(:type => 'DailyHighScore')
    HighScore.create!(:type => 'WeeklyHighScore')
    
    splitter = SingleTableInheritanceSplitter.new(HighScore)
    assert_equal HighScore, splitter.source
    assert_equal({'DailyHighScore' => 'daily_high_scores', 'WeeklyHighScore' => 'weekly_high_scores'}, splitter.destinations)
  end
  
  def test_split_moves_data_to_correct_tables
    daily = HighScore.create!(:type => 'DailyHighScore', :value => 2)
    weekly = HighScore.create!(:type => 'WeeklyHighScore', :value => 3)
    
    splitter = SingleTableInheritanceSplitter.new(HighScore)
    splitter.cleave!
    
    assert_equal [2], DailyHighScore.find(:all).map(&:value)
    assert_equal [3], WeeklyHighScore.find(:all).map(&:value)
  end
end