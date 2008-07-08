class SingleTableInheritanceCleaver
  attr_accessor :source, :destinations, :chunk_size

  DISALLOWED_COLUMN_NAMES = %w(id type)

  def initialize(source, options = {})
    self.source = source
    
    all_types = source.find(:all, :select => 'DISTINCT type').map {|t| t.attributes['type']}
    self.destinations = {}
    all_types.each do |type|
      self.destinations[type] = type.tableize
    end
    
    self.chunk_size = options[:chunk_size]
  end
  
  # Process records from the source table into the destination tables
  def cleave!
    destinations.each do |source_type, destination_table_name|
      # "insert into games(uploaded_by_id, title) select uploaded_by_id, title from games where id=-1;"
      ActiveRecord::Base.connection.insert <<-SQL
        INSERT INTO #{destination_table_name}(#{column_names}) SELECT #{column_names} FROM #{source.table_name} AS source_table WHERE source_table.type = '#{source_type}'
      SQL
    end
  end

  def column_names
    names = self.source.column_names
    
    names.delete_if { |name| DISALLOWED_COLUMN_NAMES.include?(name) }
    names.join(', ')
  end
end
