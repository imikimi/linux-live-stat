#!/usr/bin/ruby
######################################################################
######################################################################
#require File.dirname(__FILE__)+'/../lib/imikimi_ruby_tools'
#{{{

#**********************************************
module Math
    def Math.log_n(n,val)
        log(val)/log(n)
    end
end
#**********************************************

def stack_trace_message
    lines=Kernel.caller
    indent=" %4d: "
    t=lines.length
    c=0
    lines.collect! {|l| a="#{indent%(t-c)}#{l}";c+=1;a}
    lines.join("\n")
end

class Exception
    def detailed_output
        "Exception #{self.class}: #{self.message}\n\n"
        "Full stack-trace:\n"+
          "    "+backtrace.join("\n    ")
    end
end

def nice(options={})
    #options
    #   :message =>
    #   :ratio => 1.0
    #   :dots => true
    message=options[:message] || "executing... "
    ratio=options[:ratio] || 1.0
    dots=options[:dots]
    outf=$stdout


    outf.write message
    outf.flush

    start_time=Time.now
    outf.write yield
    outf.flush
    end_time=Time.now

    total_time=end_time-start_time
    sleep_seconds=total_time*ratio

    if sleep_seconds>0
        outf.write " | #{'%.1f'%total_time}s (#{'%.1f'%ratio}x sleep)"
        outf.flush

        if dots
            while sleep_seconds>0
                outf.write(".");
                outf.flush();
                sleep(min(1,sleep_seconds));
                sleep_seconds-=1
            end
        else
            sleep sleep_seconds
        end
    end
    outf.write "\n"
end

def timed_do(options={})
    #options:
    #   :output => :log, :one_line, :console, :return
    #       one_line - assumes block doesn't output anything; puts the description out first, then the time
    #   :description => ""
    #   :precision=>2
    #   :nice=>true or Num where sleep time will be Num*duration
    output=options[:output] || :console
    precision=(options[:precision] || 2).to_i
    description=options[:description] || "Block"
    nice=options[:nice]
    nice=1.0 if nice==true

    start_time=Time.now
    if output==:one_line
      $stdout.write description
      $stdout.write " ..."
      $stdout.flush
      description=""
    end
    ret=yield
    end_time=Time.now
    total_time=end_time-start_time

    #wait total_time*nice seconds if "nice"
    sleep total_time*nice if nice

    #generate output
    str="#{description} took: #{"%.#{precision}f"%total_time}s"
    case output
    when :log then logger.info str
    when :one_line,:console then puts str
    when :return then return {:result=>ret,:start_time=>start_time, :end_time=>end_time, :total_time=>total_time, :output=>str}
    else raise ArgumentError.new("invalid output mode: #{output}")
    end

    ret
end


def abs_path(*args)
    #Source Relative File name
    #Does 3 things:
    #   If the first arg is an existing file, changes it to refer to the dir the file is in instead of the file
    #       this allows you to pass in __FILE__
    #
    #   Joins all args into a path.
    #   Makes that path absolute
    args=args[0] if args[0].kind_of?(Array)
    args[0]=File.dirname(args[0]) if File.exist?(args[0]) && !File.stat(args[0]).directory?
    File.expand_path File.join(args)
end

class LocalFileStat
    def initialize
        @time_delta=0
        @total_count=0
        @total_size=0
        @waste=0
    end

    def add(time_delta,size,waste)
        @total_count+=1
        @time_delta+=time_delta
        @total_size+=size
        @waste+=waste
    end

    def to_s
        return "0 files" if @total_count==0
        sprintf(
            "%s files, %s bytes, %sb av, %sb wasted (%.1f%%), avg age: %s",
            commaize(@total_count),
            commaize(@total_size),
            commaize(@total_size/@total_count),
            commaize(@waste),
            100.0*@waste.to_f/(@waste+@total_size),
            get_time_range_string(@time_delta/@total_count)
            )
    end
end

def generate_random_string(length)
    chars = ("A".."Z").to_a + ("a".."z").to_a + ("1".."9").to_a #62 different values
    Array.new(length, '').collect{chars[rand(chars.size)]}.join
end

def mergeRecordListsUnique(l1,l2)
    ret=[]
    temp_hash={}
    l1.each do |c|
        if !temp_hash[c.id]
            temp_hash[c.id]=c
            ret << c
        end
    end
    l2.each do |c|
        if !temp_hash[c.id]
            temp_hash[c.id]=c
            ret << c
        end
    end
    return ret
end

def min(num1,num2)
    return num1 || num2 if !num1 || !num2
    return num1 < num2 ? num1 : num2
end
alias :minimum :min

def max(num1,num2)
    return num1 || num2 if !num1 || !num2
    return num1 > num2 ? num1 : num2
end
alias :maximum :max

def bound(a,b,c)
    #returns The closest number to B within the range [A,C]
    #if C<A, A is returned
    #if nil values for "a" and "b" are effectively "wild cards" or "unbounded"
    #nil value for b will cause nil to be returned
    return min(b,c) if !a
    return max(a,b) if !c
    return nil if !b
    ret=if b<a || c<a
            a
        elsif b>c
            c
        else
            b
        end
    return ret
end

def niceTimeDelta(seconds)
    mins=(seconds/60).to_i
    if mins > 60
        hours=mins/60
        if hours>=24
            days=hours/24
            if days>=30
                months=days/30
                pluralize((months).to_i,"month","months")
            else
                pluralize((hours/24).to_i,"day","days")
            end
        else
            pluralize((mins/60).to_i,"hour","hours")
        end
    else
        pluralize(mins,"minute","minutes")
    end
end

def interesting_methods(obj)
    m1=obj.methods
    m2=Object.new.methods   #baseline

    m2hash={}
    m2.each {|method| m2hash[method]=true}
    m1.sort!
    ret=[]
    m1.each {|method| ret<<method if !m2hash[method]}
    return ret
end

def put_interesting_methods(obj)
    puts interesting_methods(obj).join("\n")
end

def commaize(a)
    return a if !a
    a=a.to_s.split('.');
    a[0].reverse.gsub(/(\d{3})/,'\1,').chomp(',').reverse+"#{'.'+a[1] if a[1]}"
end

class Fixnum
  def commaize
    a=self
    return a if !a
    a=a.to_s.split('.');
    a[0].reverse.gsub(/(\d{3})/,'\1,').chomp(',').reverse+"#{'.'+a[1] if a[1]}"
  end
end

#****************************************
# Adding random_sort to Array class
#****************************************

class Date

  # return the date of the first day of the week
  def first_of_week
    self - self.wday
  end

  # return the date of the last day of the week
  def last_of_week
    first_of_week+6
  end

  # return the date of the first day of next week
  def first_of_next_week
    first_of_week+7
  end

  # return the date of the first day of the month
  def first_of_month
    self - (self.day-1)
  end

  # return the date of the last day of the month
  def last_of_month
    first_of_next_month-1
  end

  # return the date of the first day of the next month
  def first_of_next_month
    # this one is more complex because months aren't all the same length
    (self+(32-self.day)).first_of_month
  end

  # return the date of the first day of the year
  def first_of_year
    self - (self.yday-1)
  end

  # return the date of the last day of the year
  def last_of_year
    first_of_next_year-1
  end

  # return the date of the first day of the next year
  def first_of_next_year
    # this one is more complex because years aren't all the same length
    (self+(367-self.yday)).first_of_year
  end
end

module Enumerable
  def threaded_map(max_threads=10)
    results = map {nil}
    ts = []
    max_threads=length if length<max_threads
    (0..max_threads-1).each do |start_offset|
      ts << Thread::new do
        i=start_offset
        while i<length
          results[i] = yield self[i]
          i+=max_threads
        end
      end
    end
    ts.each &:join
    results
  end
end

class Array
    include Comparable

    def select_random
        self[Kernel.rand(length)]
    end

    def unique
        vals={}
        ret=[]
        self.each do |v|
            ret<<v if !vals.key?(v)
            vals[v]=true
        end
        return ret
    end

    def random_sort!(first_n=nil)
        first_n=first_n || length
        each_index do |i|
            return if i>=first_n
            j = Kernel.rand(length-i) + i
            self[j], self[i] = self[i], self[j]
        end
        self
    end

    def <=>(other)
        len=minimum(length,other.length)
        (0..len-1).each do |i|
            v1=self[i]
            v2=other[i]
            return 0 if !v1 && !v2
            return -1 if !v1
            return 1 if !v2
            val=v1<=>v2
            return val if val!=0
        end
        return length<=>other.length
    end

    def random_sort(first_n=nil)
        dup.random_sort!(first_n)
    end

    def to_hash #returns a hash from value to index (also, means a hash from value to true because of ruby's truth semantics)
        ret={}
        each_index {|i| ret[self[i]]=i }
        return ret
    end
end

# Converts an IP string to integer
def ip2int(ip)
  return 0 unless ip =~ /\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/

  v = ip.split('.').collect { |i| i.to_i }
  return (v[0] << 24) | (v[1] << 16) | (v[2] << 8 ) | (v[3]);
end


# Converts an integer to IP string... could be prettier
def int2ip(int)
  tmp = int.to_i
  parts = []

  3.times do |i|
    tmp = tmp / 256.0
    parts << (256 * (tmp - tmp.to_i)).to_i
  end

  parts << tmp.to_i
  parts.reverse.join('.')
end

alias ip_to_i ip2int
alias i_to_ip int2ip
#}}}
######################################################################
######################################################################
######################################################################

######################################################################
######################################################################
#require File.dirname(__FILE__)+'/../lib/linux_lib'
#{{{

$mac_osx=(RUBY_PLATFORM =~ /darwin/)
def mac_osx
  $mac_osx
end

def cpu_type
    cpuinfo=File.open("/proc/cpuinfo").read()
    proc=nil
    procs=[]
    keep_list=
        {
        :cpu_MHz=>true,
        :cache_size=>true,
        :model_name=>true,
        :bogomips=>true,
        }
    cpuinfo.each_line do |line|
        next if line.strip==""
        pair=line.split(":",2).collect {|c| c.strip}
        proc=pair[1].to_i if pair[0]=="processor"
        procs[proc]||={}
        key=pair[0].gsub(/\s+/,"_").intern
        if keep_list[key]
            value=pair[1]
            value=value.to_f if value[/^[\d]+(.[\d]+)?$/]
            procs[proc][key]=value
        end
    end
    final_procs={}
    procs.each do |proc|
        proc[:cpu_GHz]=proc[:model_name][/[\d]+.[\d]+GHz/].to_f
        key=proc[:model_name].gsub(/[\s]+/," ")
        final_procs[key]={:cores=>0} if !final_procs[key]
        final_procs[key][:cores]+=1
        final_procs[key].merge!(proc)
        final_procs[key].delete :model_name
    end
    final_procs
end

def local_disk_usage
    getDfStats
end

def getDfStats(path=nil)
    df_command = if mac_osx
      "df -k -l"
    else
      "df -BK -l"
    end
    if path
        path=File.expand_path(path)
        df_command+=" #{path}"
    end
    df_output=`#{df_command}`    # - l=local file systems
    lines=[]
    all={:hash => {}, :array => [], :worst => nil, :total => {:percent=>0, :used=>0, :free=>0, :total=>0} }
    line_carry_over=""  #if the first field is very long, df splits the info over two lines
    df_output.each_line do |line|
        line=line_carry_over+line
        line_carry_over=""
        if !line[/Filesystem/]
            fields=line.split(/\s+/)
            if fields.length==1
                line_carry_over=line.strip+" "
                next
            end
            data={}
            curpath=File.expand_path(fields[5])
            data[:device]=fields[0]
            data[:total]=fields[1].to_i*1024
            data[:used]=fields[2].to_i*1024
            data[:free]=fields[3].to_i*1024
            data[:percent]=fields[4].to_i
            data[:path]=curpath
            all[:array]<<data
            all[:hash][curpath]=data
            all[:worst]=data if !all[:worst] || all[:worst][:percent] < data[:percent]
            all[:total][:used]+=data[:used]
            all[:total][:free]+=data[:free]
            all[:total][:total]+=data[:total]
            all[:total][:percent]+=(100*all[:total][:used])/all[:total][:total]
        end
    end
    return all[:array][0] if path   #only return the first (and only) result if path specified
    all
end

def getProcCommandLine(pid)
    begin
        File.new("/proc/#{pid}/cmdline").read.gsub("\000"," ").strip
    rescue
        ""
    end
end

def getProcList(cmdline_regex=nil)
    d=Dir.new("/proc")
    ret=[]
    d.each do |dir|
        if dir=~/^\d+$/
            #dir is a process
            pid=dir.to_i
            match= !cmdline_regex || getProcCommandLine(pid)=~cmdline_regex
            ret.push(dir) if match
        end
    end
    ret
end

def killProc(pid,force=false)   #pid can also be an array
    pid_array=pid
    pid_array=[pid] if !pid.kind_of?(Array)

    ret="blah"
    pid_array.each do |pid|
        output=`kill #{"-KILL" if force} #{pid}`
        if !$?.success?
            raise output
        end
    end
end

def getProcMemInfo
    process_path="/proc/#{Process.pid}/"
    status=File.new(process_path+"status")

    readmap={
        "VmSize" => :virtual,
        "VmLck" => :locked,
        "VmRSS" => :resident,
        "VmData" => :data,
        "VmStk" => :stack,
        "VmExe" => :code,
        "VmLib" => :lib,
        }
    ret=Hash.new
    status.each_line do |line|
        a=line.split(":")
        key=a[0].strip
        value=a[1].strip
        if readmap[key]
            ret[readmap[key]]=value.to_i*1024
        end
    end
    status.close
    ret
end


def getLoadStats()
    f=File.new("/proc/loadavg")
    a=f.read.strip.split(/\s+/)
    stats=Hash.new
    stats[:load_avg_1min]=a[0]
    stats[:load_avg_5min]=a[1]
    stats[:load_avg_15min]=a[2]
    stats
end

def getMemStats()
    f=File.new("/proc/meminfo")
    stats=Hash.new
    f.each_line do |line|
        a=line.split(":")
        key=a[0].chomp
        value=a[1].split(/\s+/)[1].to_i
        stats[key]=value
    end
    stats
end

def getVMStats()
    f=File.new("/proc/vmstat")
    stats=Hash.new
    f.each_line do |line|
        a=line.split(/\s+/)
        key=a[0].strip
        value=a[1].to_i
        stats[key]=value
    end
    stats
end

def getNetStats()
    f=File.new("/proc/net/dev")
    all_stats=Hash.new
    f.each_line do |line|
        devline=line.split(":")
        if devline.length==2

            device=devline[0].strip
            next if device["bond"] # skip linux bond devices
            fields=devline[1].strip.split(/\s+/)

            #receive [0-7]:    bytes packets errs drop fifo frame compressed multicast
            #transmit [8-15]:    bytes packets errs drop fifo colls carrier compressed
            stats=Hash.new
            stats[:in_bytes]=fields[0].to_i
            stats[:in_packets]=fields[1].to_i
            stats[:in_errs]=fields[2].to_i
            stats[:in_drop]=fields[3].to_i

            stats[:out_bytes]=fields[8].to_i
            stats[:out_packets]=fields[9].to_i
            stats[:out_errs]=fields[10].to_i
            stats[:out_drop]=fields[11].to_i

            all_stats[device]=stats
        end
    end
    all_stats
end

def getTCPConnections
    f=File.new("/proc/net/tcp")
    num_conns=0
    f.each_line do |line|
        num=line.split(": ")[0].to_i
        num_conns=num if num > num_conns
    end
    num_conns
end

def getDiskStats()
    #http://www.mjmwired.net/kernel/Documentation/iostats.txt
    f=File.new("/proc/diskstats")
    all_stats=Hash.new
    f.each_line do |line|
        fields=line.split(/\s+/)

        device=fields[3].strip
        fields=fields[4..-1]    #effectively delete the first 4 fields
        if device=~/^(sd|hd)[a-z]$/
            stats=Hash.new

            stats[:reads_issued]=fields[0].to_i
            stats[:reads_merged]=fields[1].to_i
            stats[:sectors_read]=fields[2].to_i
            stats[:time_reading]=fields[3].to_i #in milliseconds

            stats[:writes_completed]=fields[4].to_i
            stats[:sectors_written]=fields[6].to_i
            stats[:time_writing]=fields[7].to_i #in milliseconds

            stats[:active_ios]=fields[8].to_i #in milliseconds

            stats[:total_time]=fields[9].to_i #in milliseconds
            stats[:weighted_total_time]=fields[10].to_i #in milliseconds

            stats[:k_read]=stats[:sectors_read]/2
            stats[:k_written]=stats[:sectors_written]/2
            all_stats[device]=stats
        end
    end
    all_stats
end

def getCPUStats(device="cpu")
    #http://www.mjmwired.net/kernel/Documentation/iostats.txt
    f=File.new("/proc/stat")
    stats=Hash.new
    f.each_line do |line|
        if line[device+" "]
            fields=line.split(device)[1].strip.split(/\s+/)

            stats[:user]=fields[0].to_f    #seconds running "normal" user processes
            stats[:nice]=fields[1].to_f    #seconds running "niced" user processes
            stats[:sys]=fields[2].to_f     #seconds executing in the kernel
            stats[:idle]=fields[3].to_f    #seconds idle
            stats[:iowait]=fields[4].to_f  #seconds waiting for I/O to complete
        end
    end
    stats
end

def statDiff(stat1,stat2)   #subtracts all values in stat2 from stat1 (both hashes)
    diffstat=Hash.new
    stat1.each do |k,v1|
        diffstat[k]=v1-stat2[k]
        diffstat[k]+=2**32 if diffstat[k]<0 #take into account roll-over on 32 bit machines, will need to change for 64 bit
    end
    diffstat
end

def multiStatDiff(stat1,stat2)   #operates on hash-sets of stat-sets (stat1 and stat2 are both hashs containing multiple instances of the datastructure statDiff expects)
    diffstat=Hash.new
    stat1.each do |k,v|
        diffstat[k]=statDiff(stat1[k],stat2[k])
    end
    diffstat
end

def mergeMultiStats(stat_set,merge_options)
    merged_stats=Hash.new
    stat_set.each do |dev,stats|
        stats.each do |k,v|
            v=v.to_i
            if !merged_stats[k]
                merged_stats[k]=v
            else
                case merge_options[k]
                when :max then merged_stats[k]=max(merged_stats[k],v)
                when :min then merged_stats[k]=min(merged_stats[k],v)
                else
                    merged_stats[k]+=v              #store the sum under the same key
                    if merge_options[k]==:new_max   #calc the max, but store it in a new key (:keyname_max)
                        key="#{k}_max".intern
                        merged_stats[key]=max(merged_stats[key] || 0,v)
                    end
                end
            end
        end
    end
    merged_stats
end

#}}}
######################################################################
######################################################################
######################################################################

######################################################################
######################################################################
# require File.dirname(__FILE__)+'/../lib/color_text_lib'
#{{{
TEXTCOLORATTR=
    {
    :reset => 0,
    :bright => 1,
    :dim => 2,
    :underline => 3,
    :blink => 5,
    :reverse => 7,
    :hidden => 8,
    }

FGTEXTCOLOR=
    {
    :black => 30,
    :red => 31,
    :green => 32,
    :yellow => 33,
    :blue => 34,
    :magenta => 35,
    :cyan => 36,
    :white => 37,
    }

BGTEXTCOLOR=
    {
    :black => 40,
    :red => 41,
    :green => 42,
    :yellow => 44,
    :blue => 44,
    :magenta => 45,
    :cyan => 46,
    :white => 47,
    }

def colorText(str,fg,bg=nil,attr=nil)
    attr_num=TEXTCOLORATTR[attr] || 0
    fg_num=FGTEXTCOLOR[fg] || 37
    bg_num=BGTEXTCOLOR[bg] || 40
    "\x1b[#{attr_num};#{fg_num};#{bg_num}m"+str+"\x1b[0m"
end

def eLColorText(str,errorlevel=:inactive)
    case errorlevel
    when 1, :ok, :active, :light    then colorText(str,:green)
    when 2, :med, :medium, :warn    then colorText(str,:yellow)
    when 3, :heavy, :error          then colorText(str,:red)
    #0, :no, :inactive reserved for:
    else            colorText(str,:white,:black,:dim)
    end
end

def condColorText(str,statOK,statACTIVE=true,statWARN=false)
    if !statOK
        colorText(str,:red)
    elsif statWARN
        colorText(str,:yellow)
    elsif statACTIVE
        colorText(str,:green)
    else
        colorText(str,:white,:black,:dim)
    end
end

#}}}
######################################################################
######################################################################
######################################################################

def show_usage_and_exit(error=nil)
    puts <<ENDUSAGE

imikimi_stat.rb

Usage: ./imikimi_stat.rb [stat_options]
    -n=#                    # of iterations (default infinite)
    -s=seconds              seconds between updates
    -tcp                    show tcp connection count (slow; reads /proc/net/tcp)
    -net                    show extended network error info
    -datafile=filename      load and save last-stats to datafile (allows cumlative stats over longer periods)
    -appendfile=filename    output non-colorized line to filename (append if exists)
    -newfile=filename       output non-colorized line to filename (overwrite if exists)
    -delta                  show time-delta between outputs
    -usage_only             just show the net/mem/disk/cpu utilization
    -all_disks              show usage info for all disks
    -df show df disk usage
    -quiet
    -w / -wide              show columns more clearly (wider output)

Info:
    NetIO is in megaBITS
    % utilization of Disk is the max utilization of all Disks
    % utilization of Net is the max utilization of all NICs
    Total Net IO is the sum of all net IO across all NICs
ENDUSAGE

  puts "\nERROR: #{error}" if error
  exit 1
end

count=0
lastdate=nil
curstats=Hash.new
laststats=nil


NetEnabled=true
MemEnabled=true
SwpEnabled=true
DiskEnabled=true
CPUEnabled=true
#DiskSpaceEnabled=false   #by default only reporting for perf_mon


stat_options=Hash.new
legal_stat_options=[
  :"?",
  :all_disks,
  :appendfile,
  :datafile,
  :delta,
  :df,
  :h,
  :help,
  :n,
  :net,
  :newfile,
  :quiet,
  :s,
  :tcp,
  :usage_only,
  :wide,
  :w,
  ].to_hash


ARGV.each do |a|

    option_match=a.match(/(--?)([a-zA-Z0-9_?]+)=?(.*)$/)
    if option_match
        option_start=option_match[1]
        option_key=option_match[2].downcase.intern
        option_value=option_match[3]
        option_value=true if option_value.empty?
        unless legal_stat_options[option_key]
          show_usage_and_exit "Invalid option: -#{option_key}=#{option_value}"
        end
        stat_options[option_key]=option_value
    end
end

quiet=stat_options[:quiet]
puts "Options: #{stat_options.inspect}" if !quiet
show_usage_and_exit if stat_options[:h] || stat_options[:help] || stat_options[:"?"]

DiskSpaceEnabled=stat_options[:df]

runcount=stat_options[:n].to_i if stat_options[:n]
delay=(stat_options[:s] || 2.0).to_f

stat_options[:NetEnabled]=NetEnabled;
stat_options[:MemEnabled]=MemEnabled;
stat_options[:SwpEnabled]=SwpEnabled;
stat_options[:DiskEnabled]=DiskEnabled;
stat_options[:CPUEnabled]=CPUEnabled;
stat_options[:DiskSpaceEnabled]=DiskSpaceEnabled;
#$delim=" | "
$delim=" "
$column_delim=stat_options[:wide] || stat_options[:w] ? " | " : ""

def getHeaders(stat_options,diskDevList,showAllDisks)
    headers="    Time#{$delim}#{$column_delim}"
    if stat_options[:usage_only]
        headers+="net#{$delim}mem#{$delim}dsk#{$delim}cpu#{$delim}#{$column_delim}"
    else
        headers+="Delta#{$delim}" if stat_options[:delta]
        if stat_options[:NetEnabled]
          headers+="NET#{$delim}  i#{$delim}  o#{$delim}"
          headers+="tcp#{$delim}"                                                   if stat_options[:tcp]
          headers+="ei#{$delim}eo#{$delim}"                                         if stat_options[:net]
          headers+=$column_delim
        end
        headers+="MEM#{$delim}cch#{$delim}#{$column_delim}"                          if stat_options[:MemEnabled]
        headers+="SWP#{$delim}#{$column_delim}"                                      if stat_options[:SwpEnabled]

        if stat_options[:DiskEnabled]
            headers+="ALL#{$delim}   read#{$delim}  write#{$delim}"                 if diskDevList.length>1
            if diskDevList.length==1 || showAllDisks
              diskDevList.each do |dev|
                  headers+="#{dev.upcase}#{$delim}   read#{$delim}  write#{$delim}"
              end
            end
            headers+=$column_delim
        end
        headers+="CPU#{$delim}wIO#{$delim}ttl#{$delim}load#{$delim}#{$column_delim}" if stat_options[:CPUEnabled]
        headers+="DF%#{$delim}used (Gb)#{$delim}#{$column_delim}"                    if stat_options[:DiskSpaceEnabled]
    end
    headers
end



outfile1=File.new(stat_options[:appendfile],"a+") if stat_options[:appendfile]
outfile2=File.new(stat_options[:newfile],"w+") if stat_options[:newfile]
puts "Mirroring output to file: #{stat_options['appendfile']}" if outfile1 && !quiet
puts "Mirroring output to file: #{stat_options['newfile']}" if outfile2 && !quiet

while true
    nowTime=Time.now
    today=Time.mktime(nowTime.year,nowTime.mon,nowTime.mday)

    #output full lastdate when it changes or when we start
    if !lastdate || lastdate!=today
        puts Time.now.strftime("(%Y-%m-%d) %A %B %D at %H:%M:%S") if !quiet
        lastdate=today
    end


    curstats=Hash.new
    curstats[:cpu]=getCPUStats
    curstats[:vm]=getVMStats
    curstats[:load]=getLoadStats
    curstats[:time]=nowTime

    curstats[:all_nics]=getNetStats()
    curstats[:all_disks]=getDiskStats()

    all_disks_list=[]
    curstats[:all_disks].each {|k,v| all_disks_list << k}
    all_disks_list.sort!

    all_nics_list=[]
    curstats[:all_nics].each {|k,v| all_nics_list << k}
    all_nics_list.sort!

    # show labels
    if (count%20)==0 && !quiet
      puts colorText(getHeaders(stat_options,all_disks_list,stat_options[:all_disks]),:white,:black,:blink)
    end

    if count==0 && stat_options[:datafile] && File.exists?(stat_options[:datafile])
        puts "Loading last stats from datafile: "+stat_options[:datafile] if !quiet
        out=File.open(stat_options[:datafile],"r") rescue nil
        filedata=out.read rescue nil
        laststats=Marshal.restore(filedata) rescue nil
        File.delete stat_options[:datafile] #with correct operation, we will overwrite the file anyway, if there is a failure, often it is due to this file being corrupt
    end

    if laststats && laststats[:all_disks] && laststats[:all_nics]
        timeDiff=curstats[:time]-laststats[:time]
        diffCPUStats=statDiff(curstats[:cpu],laststats[:cpu])
        diffVMStats=statDiff(curstats[:vm],laststats[:vm])

        diffAllNicStats=multiStatDiff(curstats[:all_nics],laststats[:all_nics])
#        diffNetStats=statDiff(curstats[:net],laststats[:net])
        diffNetStats=mergeMultiStats(diffAllNicStats,{:in_bytes => :new_max, :out_bytes => :new_max})

        diffAllDiskStats=multiStatDiff(curstats[:all_disks],laststats[:all_disks])
        diffDiskStats=mergeMultiStats(diffAllDiskStats,{:total_time => :max})

        netMul=8/(timeDiff*1024*1024)
        neterrors=diffNetStats[:in_errs]+diffNetStats[:out_errs]
        memStats=getMemStats

        cpuMul=100.0/
            (
            diffCPUStats[:user]+
            diffCPUStats[:nice]+
            diffCPUStats[:sys]+
            diffCPUStats[:iowait]+
            diffCPUStats[:idle]
            )

        swapin=diffVMStats["pswpin"]
        swapout=diffVMStats["pswpout"]

        #usage stats
        netUsage=min(999,max(diffNetStats[:in_bytes_max]*netMul,diffNetStats[:out_bytes_max]*netMul))/10

        memUsage=min(99,(100.0*(1.0-((memStats["MemFree"]+memStats["Buffers"]+memStats["Cached"]+(memStats["SReclaimable"]||0)).to_f/memStats["MemTotal"].to_f))).to_i)
        cpuUsage=min(99,cpuMul*(diffCPUStats[:user]+diffCPUStats[:nice]+diffCPUStats[:sys]))

        allDisksUsage=Hash.new
        diskUsage=0
        diffAllDiskStats.each do |k,v|
            allDisksUsage[k]=min(99,(v[:total_time].to_f/(10*timeDiff)).to_i)
            diskUsage=max(diskUsage,allDisksUsage[k])
        end

        #Grey->Green->Yellow->Red level tests
        netOK=netUsage<90
        netWARN=netUsage>75
        netACTIVE=netUsage>=1

        diskOK=diskUsage<95
        diskWARN=diskUsage>50
        diskACTIVE=diskUsage>0

        memOK=memUsage<90
        memWARN=memUsage>66
        memACTIVE=memUsage>10

        cpuOK=cpuMul*diffCPUStats[:idle]>=10
        cpuWARN=cpuMul*diffCPUStats[:idle]<=50
        cpuACTIVE=cpuMul*diffCPUStats[:idle]<=90

        vmOK=swapin==0 && swapout==0

        #Outline
        outline=nowTime.strftime("%H:%M:%S#{$delim}#{$column_delim}")
        if stat_options[:usage_only]
            outline+=condColorText(sprintf("%2d%%#{$delim}",netUsage),netOK,netACTIVE,netWARN)
            outline+=condColorText(sprintf("%2d%%#{$delim}",memUsage),memOK,memACTIVE,memWARN)
            outline+=condColorText(sprintf("%2d%%#{$delim}",diskUsage),diskOK,diskACTIVE,diskWARN)
            outline+=condColorText(sprintf("%2d%%#{$delim}",cpuUsage),cpuOK,cpuACTIVE,cpuWARN)
            outline+=$column_delim
        else
            #Detailed Mem Stats
            totalMem=memStats["MemTotal"]
            memline=sprintf("%2d%%#{$delim}%2d%%#{$delim}",
                memUsage,
                100.0*memStats["Cached"]/totalMem,
                100.0*(memStats["SwapTotal"]-memStats["SwapFree"])/totalMem
                )


            #Detailed VM Stats
            vmline=sprintf("%3d#{$delim}",swapin+swapout)

            #Detailed CPU Stats
            cpuline=sprintf("%2d%%#{$delim}%2d%%#{$delim}%2d%%#{$delim}%4.1f#{$delim}",
                cpuUsage,
                min(cpuMul*diffCPUStats[:iowait],99),
                min(100-cpuMul*diffCPUStats[:idle],99),
                curstats[:load][:load_avg_1min]
                )

            #Detailed Net Stats
            netline=sprintf("%2d%%#{$delim}%3d#{$delim}%3d#{$delim}",
                netUsage,
                diffNetStats[:in_bytes]*netMul,
                diffNetStats[:out_bytes]*netMul
                )
            netline+=sprintf("%3d#{$delim}",getTCPConnections ) if stat_options[:tcp]
            netline+=sprintf("%2d#{$delim}%2d#{$delim}",diffNetStats[:in_errs],diffNetStats[:out_errs]) if stat_options[:net]


            deltaLine=sprintf("%5.2f#{$delim}",timeDiff.to_s)
            netErrorLine="#{neterrors} net errors!#{$delim}"

            outline+=deltaLine if stat_options[:delta]
            outline+=condColorText(netline,netOK,netACTIVE,netWARN)+$column_delim   if NetEnabled
            outline+=condColorText(memline,memOK,memACTIVE,memWARN)+$column_delim   if MemEnabled
            outline+=condColorText(vmline,vmOK,false)+$column_delim                 if SwpEnabled

            if DiskEnabled
                def getDiskLine(usage,local_stats)
                    diskline=sprintf("%2d%%#{$delim}%6dk#{$delim}%6dk#{$delim}",
                        usage,
                        local_stats[:k_read],
                        local_stats[:k_written]
                        )
                    diskOK=usage<95
                    diskWARN=usage>50
                    diskACTIVE=usage>0
                    return condColorText(diskline,diskOK,diskACTIVE,diskWARN)
                end
                outline+=getDiskLine(diskUsage,diffDiskStats) if all_disks_list.length>1
                if all_disks_list.length==1 || stat_options[:all_disks]
                all_disks_list.each do |dev|
                    #Detailed Disk Stats
                    outline+=getDiskLine(allDisksUsage[dev],diffAllDiskStats[dev])
                end
                end
                outline+=$column_delim
            end

            outline+=condColorText(cpuline,cpuOK,cpuACTIVE,cpuWARN)+$column_delim   if CPUEnabled

            if DiskSpaceEnabled
                #DiskUsage Stats

                du_stats=getDfStats
                du_total=du_stats[:total]
                du_percent=du_stats[:worst][:percent]
                dfline=sprintf("%2d%%#{$delim}%9s#{$delim}",
                    min(du_percent,99),
                    "#{du_total[:used]/(1024*1024*1024)}/#{du_total[:total]/(1024*1024*1024)}"
                    )


                outline+=condColorText(dfline,du_percent<95,du_percent>=50,du_percent>=90)+$column_delim
            end

            outline+=colorText(netErrorLine,:red)+$column_delim if neterrors>0
        end
        $stdout.puts outline
        $stdout.flush

        if outfile1 || outfile2

            du_stats=getDfStats
            du_total=du_stats[:total]
            du_percent=du_stats[:worst][:percent]
            du_path=du_stats[:worst][:path]
            du_path=du_path.split("/")[-1] if du_path!="/"

            outline=nowTime.strftime("%Y-%m-%d %H:%M:%S")+" |"+
                sprintf(" %10.2f |",timeDiff.to_s)+
                sprintf(" NMDC | %2d%% | %2d%% | %2d%% | %2d%% |",netUsage,memUsage,diskUsage,cpuUsage)+
                sprintf(" NET | %5.2f | %5.2f | %2d | %2d |",diffNetStats[:in_bytes]*netMul,diffNetStats[:out_bytes]*netMul,diffNetStats[:in_errs],diffNetStats[:out_errs])+
                sprintf(" MEM | %8d | %8d | %8d | %8d | %8d |",memStats["MemTotal"],memStats["MemFree"],memStats["Buffers"],memStats["Cached"],memStats["Active"])+
                sprintf(" VM | %3d | %3d |",diffVMStats["pswpin"],diffVMStats["pswpout"])+
                sprintf(" %s | %5dk | %5dk |",
                    diffAllDiskStats.length >1 ? "#{diffAllDiskStats.length}hd" : all_disks_list[0].downcase,
                    diffDiskStats[:k_read],diffDiskStats[:k_written])+
                sprintf(" CPU | %2d%% | %2d%% | %2d%% | %2d%% | %4.1f |",
                    min(cpuMul*diffCPUStats[:user],99),
                    min(cpuMul*diffCPUStats[:sys],99),
                    min(cpuMul*diffCPUStats[:iowait],99),
                    min(100-cpuMul*diffCPUStats[:idle],99),
                    curstats[:load][:load_avg_1min]
                    )+
                sprintf(" %s | %2d%% | %9s |",
                    du_path,
                    min(du_percent,99),
                    "#{du_total[:used]/(1024*1024*1024)}/#{du_total[:total]/(1024*1024*1024)}"
                    )
            if outfile1
                outfile1.puts outline
                outfile1.flush
            end
            if outfile2
                outfile2.puts outline
                outfile2.flush
            end
        end

        break if runcount && count>=runcount
    end
    laststats=curstats
    count+=1
    sleep delay
end
outfile1.close if outfile1
outfile2.close if outfile2
if stat_options[:datafile]
    puts "Saving last stats to datafile: "+stat_options[:datafile] if !quiet
    out=File.open(stat_options[:datafile],"w")
    out.write(Marshal.dump(curstats))
    out.close
end
