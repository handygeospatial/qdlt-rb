require 'open-uri'
require 'digest/md5'
require 'fileutils'
require 'zlib'
require 'thread'
require 'time'

Z_EXTENT = (0..18)
T = 'std'
N_THREADS = 8
Q_SIZE = 200
WAIT = 5
ALL = 50749203
CONTINUE = nil#41895729

$threads = Array.new(N_THREADS)
$status = {:skip => 0, :ok => 0, :ng => 0, :path => nil}
$q = SizedQueue.new(Q_SIZE)

$threads.size.times {|i|
  $threads[i] = Thread.new(i) do
    while o = $q.pop
      buf = open(o[:url]).read
      buf_md5 = Digest::MD5.hexdigest(buf)
      if o[:md5] != buf_md5
        $status[:ng] += 1
        #FileUtils.rm(o[:path]) if File.exist?(o[:path])
      else
        [File.dirname(o[:path])].each{|it|
          FileUtils.mkdir_p(it) unless File.directory?(it)
        }
        if(File.exist?(o[:path]))
          bk_path = "bak/#{o[:path]}"
          bk_path.insert(bk_path.rindex('.'),
            ".#{File.mtime(o[:path]).iso8601.split('T')[0].gsub('-', '')}")
          [File.dirname(bk_path)].each {|it|
            FileUtils.mkdir_p(it) unless File.directory?(it)
          }
          FileUtils.cp(o[:path], bk_path, :preserve => true)
        end
        File.open("#{o[:path]}", 'w') {|w| w.print buf}
        File.utime(o[:date], o[:date], o[:path])
        $status[:ok] += 1
      end
    end
  end
}
watcher = Thread.new do
  while $threads.reduce(false) {|any_alive, t| any_alive or t.alive?}
    last_status = $status.clone
    sleep WAIT
    print <<-EOS
#{Time.now.iso8601[11..18]} #{$status[:path]} #{$q.size} \
#{%w{skip ok ng}.map{|k| ($status[k.to_sym] - last_status[k.to_sym]) / WAIT}}\
/s #{%w{skip ok ng}.map{|k| $status[k.to_sym]}} #{$count} #{(100.0 * $count / ALL).to_i}%
    EOS
  end
end

$count = 0
Zlib::GzipReader.open('mokuroku.csv.gz').each_line {|l|
  $count += 1
  (path, date, size, md5) = l.strip.split(',')
  date = date.to_i
  url = "http://cyberjapandata.gsi.go.jp/xyz/#{T}/#{path}"
  $status[:path] = path
  if ((CONTINUE ? $count < CONTINUE : false) ||
      !Z_EXTENT.include?(path.split('/')[0].to_i)) ||
      (File.exist?("#{path}") && Digest::MD5.file(path) == md5)
    $status[:skip] += 1
    next
  end
  $q.push({:url => url, :date => date, :md5 => md5, :path => path})
}

$threads.size.times {|i| $q.push(nil)}

$threads.each {|t| t.join}
watcher.join
