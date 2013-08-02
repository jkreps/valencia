#!/usr/bin/Rscript

options(scipen=6)
opts <- commandArgs(trailingOnly = T);
if(length(opts) != 2)
    stop("USAGE: draw-run.R input_file output_dir");
input_file <- opts[1];
output <- opts[2];

draw.to <- function(output_dir, fname) {
 png(paste(output_dir, paste(fname, '.png', sep=''), sep="/"), width=500, height=500)
}

vals<-read.table(input_file, header=T)
gbs <- vals$log_size/(1024*1024*1024)

draw.to(output, 'throughput_vs_time');
plot(vals$all_qps, ylim=c(0, max(vals$all_qps)), type='l', col='red', xlab='Time', xaxt='n', ylab="Requests per second", main='Throughput Over Time');
dev.off();

draw.to(output, 'throughput_vs_size');
plot(gbs, vals$all_qps, ylim=c(0, max(vals$all_qps)), col='red', xlab='Data Size (GBs)', ylab="Requests per second", main='Throughput vs. Data Size');
dev.off();

draw.to(output, 'latency_vs_size');
max_y <- max(vals$read_99th, vals$write_99th);
plot(gbs, vals$read_95th, ylim=c(0, max_y), col='red', xlab='Data Size (GBs)', ylab="Latency (ms)", main='Latency vs. Data Size');
points(gbs, vals$read_99th, col='blue')
points(gbs, vals$write_95th, col='green')
points(gbs, vals$write_99th, col='yellow')
legend(min(gbs), max_y, c("95th Perc. Read","99th Perc. Read", "95th Perc. Write", "99th Perc. Write"), col=c("red", "blue","green", "yellow"), lty=1);
dev.off();

draw.to(output, 'latency_vs_time');
max_y <- max(vals$read_99th, vals$write_99th);
plot(vals$read_95th, ylim=c(0, max_y), col='red', type='l', xlab='Time', xaxt='n', ylab="Latency (ms)", main='Latency vs. Time');
lines(vals$read_99th, col='blue')
lines(vals$write_95th, col='green')
lines(vals$write_99th, col='yellow')
legend(min(gbs), max_y, c("95th Perc. Read","99th Perc. Read", "95th Perc. Write", "99th Perc. Write"), col=c("red", "blue","green", "yellow"), lty=1);
dev.off();

draw.to(output, 'index_size');
plot(vals$idx_size/1000000, ylim=c(0, max(vals$idx_size/1000000)), type='l', col='red', xlab='Time', xaxt='n', ylab="Index Size (Millions)", main='Index Size Vs Time');
dev.off();

draw.to(output, 'gc_processed');
plot(vals$gc_rate/(1024*1024*1024), ylim=c(0, max(vals$gc_rate/(1024*1024*1024))), type='l', col='red', xlab='Time', xaxt='n', ylab="GB/Second Processed", main='Cleaner Throughput');
dev.off();

draw.to(output, 'gc_backlog');
plot(vals$gc_backlog, ylim=c(0, max(vals$gc_backlog)), type='l', col='red', xlab='Time', xaxt='n', ylab="Backlogged Segments", main='Cleaner Backlog');
dev.off();

draw.to(output, 'log_utilization');
plot(100*vals$log_utilized, ylim=c(0, max(100*vals$log_utilized)), type='l', col='red', xlab='Time', xaxt='n', ylab="Log Utilization Percent", main='Log Utilization');
dev.off();

draw.to(output, 'scan_rate');
plot(vals$scan_rate/(1024*1024), ylim=c(0, max(vals$scan_rate/(1024*1024))), type='l', col='red', xlab='Time', xaxt='n', ylab="MB/Second Scanned", main='Scan Throughput');
dev.off();