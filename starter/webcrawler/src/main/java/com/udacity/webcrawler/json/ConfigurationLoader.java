package com.udacity.webcrawler.json;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
// import com.fasterxml.jackson;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationCollector;

/**
 * A static utility class that loads a JSON configuration file.
 */
public final class ConfigurationLoader {

  private final Path path;

  /**
   * Create a {@link ConfigurationLoader} that loads configuration from the given {@link Path}.
   */
  public ConfigurationLoader(Path path) {
    this.path = Objects.requireNonNull(path);
  }

  /**
   * Loads configuration from this {@link ConfigurationLoader}'s path
   *
   * @return the loaded {@link CrawlerConfiguration}.
   */
  public CrawlerConfiguration load() {
    try (Reader reader =
                 Files.newBufferedReader(path)) {
      CrawlerConfiguration config = read(reader);
      reader.close();
      return config;
    } catch(Exception e){

    }
    return null;
  }

  /**
   * Loads crawler configuration from the given reader.
   *
   * @param reader a Reader pointing to a JSON string that contains crawler configuration.
   * @return a crawler configuration
   */
  public static CrawlerConfiguration read(Reader reader)  {
    // This is here to get rid of the unused variable warning.
    // TODO: Fill in this method
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
try{
      return objectMapper.readValue(reader, CrawlerConfiguration.Builder.class).build();}
catch(Exception e){

}
return null;
  }
}
