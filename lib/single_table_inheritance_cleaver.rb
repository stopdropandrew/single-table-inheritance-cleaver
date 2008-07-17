class SingleTableInheritanceCleaver
  attr_accessor :source, :destinations, :chunk_size, :rejections, :conditions, :excluded_types, :table_name_to_class_hash,
    :output

  DISALLOWED_COLUMN_NAMES = %w(id type)

  class SourceClass < ActiveRecord::Base
  end
  
  class DestinationClass < ActiveRecord::Base
  end

  def initialize(source, options = {})
    SourceClass.table_name = source.to_s.tableize
    self.source = source

    self.rejections = options[:rejections] || {}
    self.chunk_size = options[:chunk_size] || 500
    self.excluded_types = options[:excluded_types] || []
    self.destinations = options[:destinations] || {}
    self.output = options[:output]

    conflicting_types = self.destinations.keys & self.excluded_types
    raise ArgumentError, "The #{conflicting_types.join(', ')} types were explicitly included and excluded, make up your mind." unless conflicting_types.blank?

    all_types = SourceClass.find(:all, :select => 'DISTINCT type AS type_name').map(&:type_name) # bypass AR's type-inference
    valid_types = all_types - self.excluded_types
    valid_types.each do |type|
      self.destinations[type] ||= type.tableize
    end

    self.table_name_to_class_hash = {}
    self.destinations.values.each { |table_name| self.table_name_to_class_hash[table_name] = table_name.classify.constantize }

    additional_conditions = options[:conditions] || {}
    self.conditions = {}
    self.destinations.each do |source_type, destination_table_name|
      self.conditions[destination_table_name] = merge_conditions(additional_conditions, :type => source_type)
    end
  end
  
  # Process records from the source table into the destination tables
  def cleave!
    status_update "Beginning cleave on #{source}"
    destinations.each do |source_type, destination_table_name|
      status_update "Cleaving #{source_type} to #{destination_table_name}"
      cleave_destination source_type, destination_table_name
    end
  end

  def cleave_destination source_type, destination_table_name, starting_id = 0
    count_conditions = merge_conditions(self.conditions[destination_table_name], [ 'id >= ?', starting_id ] )
    total_records_to_cleave = SourceClass.count('1', :conditions => count_conditions)
    return if total_records_to_cleave.zero?

    total_chunks_to_cleave = total_records_to_cleave / chunk_size
    total_chunks_to_cleave = 1 if total_chunks_to_cleave.zero?
    output_interval = total_chunks_to_cleave / 100
    output_interval = 1 if output_interval.zero?
    status_update "#{total_records_to_cleave} more #{destination_table_name} to cleave"
    chunks_cleaved = 0
    while (starting_id = cleave_chunk(source_type, destination_table_name, starting_id))
      chunks_cleaved += 1
      percent = chunks_cleaved * 100 / total_chunks_to_cleave
      status_update "[#{chunks_cleaved}/#{total_chunks_to_cleave} #{percent}%]" if 0 == chunks_cleaved % output_interval
    end
  end
  
  def cleave_chunk source_type, destination_table_name, starting_id = 0
    return nil unless self.destinations.keys.include?(source_type)
    
    DestinationClass.set_table_name destination_table_name
    previous_max = DestinationClass.maximum('id')
    column_names = column_names(destination_table_name)
    
    conditions = merge_conditions(self.conditions[destination_table_name], [ 'id >= ?', starting_id ] )
    sql_column_names = column_names.join(', ')

    sql = [
      'INSERT INTO ', destination_table_name,
      '(', sql_column_names, ') ',
      job_select(sql_column_names, conditions)
    ].join

    SourceClass.connection.insert sql
    current_max = DestinationClass.maximum('id')

    return false unless current_max.to_i != previous_max.to_i

    last_id_processed = SourceClass.connection.execute(job_select('id', conditions)).map do |r| r['id'].to_i end.max
    last_id_processed + 1
  end

  def job_select columns, conditions
    [
      'SELECT ', columns,
      ' FROM ', SourceClass.table_name,
      ' WHERE ', conditions,
      ' ORDER BY id',
      ' LIMIT ', chunk_size
    ].join
  end

  def column_names(destination_table_name)
    names = SourceClass.columns.map(&:name)
    
    names.delete_if { |name| DISALLOWED_COLUMN_NAMES.include?(name) || Array(self.rejections[destination_table_name]).include?(name) }
    names = names & self.table_name_to_class_hash[destination_table_name].column_names
    names
  end

  def status_update what
    puts [ Time.now, ': ', what ].join if output
  end

  def merge_conditions condition1, condition2
    SourceClass.send(:merge_conditions, condition1, condition2)
  end

end
