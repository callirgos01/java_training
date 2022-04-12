package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.naming.spi.DirStateFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.synchronizedMap;
import static java.util.Collections.synchronizedSet;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {


  private class CrawlInternalAction extends RecursiveAction{
    Instant deadline;
    String url;
    int maxDepth;
    Map<String, Integer> counts;
    Set<String> visitedUrls;
    private final List<Pattern> ignoredUrls;
    private final PageParserFactory parserFactory;
    private final ForkJoinPool pool;

    public CrawlInternalAction( Instant deadline,  String url, int maxDepth,  Map<String, Integer> counts,
                                 Set<String> visitedUrls,
                                List<Pattern> ignoredUrls, PageParserFactory parserFactory, ForkJoinPool pool) {
      this.deadline = deadline;
      this.url = url;
      this.maxDepth = maxDepth;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
      this.ignoredUrls = ignoredUrls;
      this.parserFactory = parserFactory;
      this.pool = pool;
    }
    @Override
    protected void compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return;
      }
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return;
        }
      }
      if (this.visitedUrls.contains(url)) {
        return;
      }
      this.visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();
      for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        if (counts.containsKey(e.getKey())) {
          counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
        } else {
          counts.put(e.getKey(), e.getValue());
        }
      }
      for (String link : result.getLinks()) {
        pool.invoke(new CrawlInternalAction( deadline, link, this.maxDepth-1, counts, visitedUrls, this.ignoredUrls, this.parserFactory, this.pool));
      }
    }
  }


  private final Clock clock;
  private final PageParserFactory parserFactory;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;

  @Inject
  ParallelWebCrawler(
      Clock clock,
      PageParserFactory parserFactory,
      @Timeout Duration timeout,
      @MaxDepth int maxDepth,
      @PopularWordCount int popularWordCount,
      @TargetParallelism int threadCount,
      @IgnoredUrls List<Pattern> ignoredUrls) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
    this.parserFactory = parserFactory;

  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    Map<String, Integer> counts = synchronizedMap(new HashMap<>());
    Set<String> visitedUrls = synchronizedSet(new HashSet<>());

    for (String url : startingUrls) {
      pool.invoke(new CrawlInternalAction( deadline, url, this.maxDepth, counts, visitedUrls, this.ignoredUrls, this.parserFactory, this.pool));
    }
    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
