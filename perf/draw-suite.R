#!/usr/bin/Rscript

options <- commandArgs(trailingOnly = T);
directory <- options[1];

read.perf <- function(fname) {
  read.table(paste(directory, fname, sep="/"), header=T);
}

draw.to <- function(fname) {
 svg(paste(directory, fname, '.svg', sep="/"))
}

latency.graph <- function(fname, main, x, xlab, yavg, y95, y99) {
  max_y <- max(yavg, y95, y99);
  min_x <- min(x);
  draw.to(fname);
  plot(x, 
       yavg, 
       ylim=c(0, max_y),
       type='l', 
       col='red', 
       xlab=xlab, 
       ylab="Latency (ms)", 
       main=main);
  lines(x, y95, col='blue');
  lines(x, y99, col='green');
  legend(min_x, max_y, c("Average","95th Percentile", "99th Percentile"), col=c("red", "blue","green"), lty=1);
  dev.off();
}

simple <- read.perf('simple.csv');

# Impact of throughput on latency
vary_throughput <- read.perf('vary_throughput.csv');
latency.graph(fname='vary_throughput', 
              main='Impact of Throughput on Latency', 
              x=vary_throughput$all_qps,
              xlab="Requests Per Second",
              yavg=vary_throughput$all_avg, 
              y95=vary_throughput$all_95th, 
              y99=vary_throughput$all_99th);

# Impact of scans on latency and throughput
scan_impact <- read.perf('scan_impact.csv');
latency.graph(fname='scan_impact',
              main='Impact of Background Scans on Latency',
              x=scan_impact$scan_rate,
              xlab="Scan Throughput",
              yavg=scan_impact$all_avg,
              yavg=scan_impact$all_95th,
              yavg=scan_impact$all_99th);

max_write <- read.perf('max_write.csv');
vary_size_fixed_throughput <- read.perf('vary_size_fixed_throughput.csv');
vary_size_max_throughput <- read.perf('vary_size_max_throughput.csv');
vary_utilization_fixed_throughput <- read.perf('vary_utilization_fixed_throughput.csv');
vary_utilization_max_throughput <- read.perf('vary_utilization_max_throughput.csv');
vary_value_size_fixed_throughput <- read.perf('vary_value_size_fixed_throughput.csv');
vary_value_size_max_throughput <- read.perf('vary_value_size_max_throughput.csv');
