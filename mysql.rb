# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "socket"

# Read rows from a (remote) MySQL database.
#
# mailto: svallero@to.infn.it
#
# Reads a list of tables from the specified DB.
#
# Any tables being watched must have an 'id' column that is monotonically
# increasing.
#
# Example of logstash config:
#
#     input {
#       mysql {
#         host => "remote_host_ip"
#         port => port (default is 3306)
#         user => "mysql_user"
#         identifier => "password"
#         database => "dbname"
#         tables => [table1,table2,table3, ... ,table10000] 
#         batch => how many rows to fetch at a time (default is 5)
#       }
#     }
#     output {
#       stdout {
#         debug => true
#       }
#     }
#
# Sample output: TODO
#

class LogStash::Inputs::Mysql < LogStash::Inputs::Base
  config_name "mysql"
  milestone 1

  # IP of remote host 
  config :host, :validate => :string, :required => true

  # Port where mysqld is listening 
  config :port, :validate => :number, :default => 3306

  # User allowed to connect to remote DB 
  config :user, :validate => :string, :required => true

  # Identifier (password) 
  config :identifier, :validate => :string, :default => ""

  # DB name
  config :database, :validate => :string, :required => true
  
  # Tables to be considered
  config :tables, :validate => :array, :default => []

  # How many rows to fetch at a time from each SELECT call.
  config :batch, :validate => :number, :default => 5

  public
  def init_placeholder_table(db, since_table)
    begin
      db.create_table :"#{since_table}" do 
        String :table
        Int    :place
      end
    rescue
      @logger.debug("since_table already exists")
    end
  end

  public
  def get_placeholder(db, since_table, table)
    since = db[:"#{since_table}"]
    x = since.where(:table => "#{table}")
    if x[:place].nil?
      init_placeholder(db, since_table, table) 
      return 0
    else
      @logger.debug("placeholder already exists, it is #{x[:place]}")
      return x[:place][:place]
    end
  end

  public 
  def init_placeholder(db, since_table, table)
    @logger.debug("init placeholder for #{table}")
    since = db[:"#{since_table}"]
    since.insert(:table => table, :place => 0)
  end

  public
  def update_placeholder(db, since_table, table, place)
    @logger.debug("set placeholder to #{place}")
    since = db[:"#{since_table}"]
    since.where(:table => table).update(:place => place)
  end
  
  public
  def get_n_rows_from_table(db, table, offset, limit)
    dataset = db["SELECT * FROM #{table}"]
    return db["SELECT * FROM #{table} WHERE (id > #{offset}) ORDER BY 'id' LIMIT #{limit}"].map { |row| row }
    print row
  end
  
  public
  def register
    require "rubygems"
    require "sequel"
    require "jdbc/mysql"
    since_table_string = "since_table_#{@user}"
    @since_table = since_table_string
    @logger.info("Registering mysql input")
    @db = Sequel.connect("jdbc:mysql://#{@host}:#{@port}/#{@database}?user=#{@user}&password=#{@identifier}") 
    @tables
    @table_data = {}
    @tables.each do |table|
      init_placeholder_table(@db, @since_table)
      last_place = get_placeholder(@db, @since_table, table)
      @table_data[table] = { :name => table, :place => last_place }	
    end
  end # def register

  public
  def run(queue)
    sleep_min = 0.01
    sleep_max = 5
    sleeptime = sleep_min

    begin
      @logger.debug("Tailing mysql db")
      loop do
        count = 0
        print @table_data 
        @table_data.each do |k, table|
        #@tables.each do |k, table|
          table_name = table[:name]
          offset = table[:place]
          @logger.debug("offset is #{offset}", :k => k, :table => table_name)
          rows = get_n_rows_from_table(@db, table_name, offset, @batch)
          count += rows.count
          rows.each do |row| 
            event = LogStash::Event.new("host" => @host, "db" => @database, "table" => table_name)
            decorate(event)
            # store each column as a field in the event.
            row.each do |column, element|
              next if column == :id
              event[column.to_s] = element
            end
            queue << event
            @table_data[k][:place] = row[:id]
          end
          # Store the last-seen row in the database
          update_placeholder(@db, @since_table, table_name, @table_data[k][:place])
        end

        if count == 0
          # nothing found in that iteration
          # sleep a bit
          @logger.debug("No new rows. Sleeping.", :time => sleeptime)
          sleeptime = [sleeptime * 2, sleep_max].min
          sleep(sleeptime)
        else
          sleeptime = sleep_min
        end
      end # loop
    end # begin/rescue
  end #run

end # class Logtstash::Inputs::Mysql


