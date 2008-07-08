class HighScore < ActiveRecord::Base
  def self.create!(options = {})
    type = options.delete(:type)
    record = super
    record.update_attribute(:type, type) if type
    record
  end
end

class LifetimeHighScore < ActiveRecord::Base
end

class DailyHighScore < ActiveRecord::Base
end

class WeeklyHighScore < ActiveRecord::Base
end

class CustomHighScore < ActiveRecord::Base
end
