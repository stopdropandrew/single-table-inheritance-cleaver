class SingleTableInheritanceCleaver
  attr_accessor :source, :destinations,
    :conditions, :rejections, :excluded_types,
    :chunk_size, :output,
    :allowed_column_names_for

  DISALLOWED_COLUMN_NAMES = %w(id type)

  class SourceClass < ActiveRecord::Base
    class << self
      public :merge_conditions, :construct_finder_sql
      def last_id_for_chunk options
        offset = options[:limit] - 1
        return false unless max_id_record = self.find(:first, options.merge(:select => 'id', :offset => offset))
        max_id_record.id
      end
    end
  end

  class DestinationClass < ActiveRecord::Base
    def self.copy_chunk table_name, starting_id, new_options = {}
      set_table_name table_name
      options = new_options.clone
      options[:conditions] = merge_conditions(new_options[:conditions], [ 'id >= ?', starting_id ] )

      columns = options.delete(:columns)
      sql_column_names = columns.join(', ')

      sql = "INSERT INTO #{table_name} (#{sql_column_names}) #{SourceClass.construct_finder_sql(options.merge(:select => sql_column_names))}"

      previous_max = self.maximum('id')
      self.connection.insert sql
      current_max = self.maximum('id')

      return false unless current_max.to_i != previous_max.to_i

      last_id_processed = SourceClass.last_id_for_chunk(options)
      return false unless last_id_processed

      last_id_processed + 1
    end
  end

  def initialize(source, options = {})
    self.source = source
    SourceClass.table_name = source.to_s

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

    self.allowed_column_names_for = {}
    self.destinations.values.each do |table_name|
      begin
        self.allowed_column_names_for[table_name] = table_name.classify.constantize.column_names
      rescue NameError
      end
    end

    additional_conditions = options[:conditions] || {}
    self.conditions = {}
    self.destinations.each do |source_type, destination_table_name|
      self.conditions[destination_table_name] = SourceClass.merge_conditions(additional_conditions, :type => source_type)
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

    DestinationClass.copy_chunk(
      destination_table_name,
      starting_id,
      :columns => column_names(destination_table_name),
      :conditions => conditions[destination_table_name],
      :limit => chunk_size
    )
  end

  def column_names(destination_table_name)
    names = SourceClass.columns.map(&:name)
    
    names.delete_if { |name| DISALLOWED_COLUMN_NAMES.include?(name) || Array(self.rejections[destination_table_name]).include?(name) }
    allowed_column_names = self.allowed_column_names_for[destination_table_name]
    names &= allowed_column_names if allowed_column_names
    names
  end

  def status_update what
    puts [ Time.now, ': ', what ].join if output
  end

  def merge_conditions *args
    SourceClass.merge_conditions *args
  end

end
