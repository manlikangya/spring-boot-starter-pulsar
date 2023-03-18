package io.github.manlikang.pulsar.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

/**
 * @author fuhan
 * @date 2022/12/1 - 18:53
 */
@Component
public class SpringHolder implements ApplicationContextAware {

  private static ApplicationContext context = null;

  @Override
  public void setApplicationContext(@Nonnull ApplicationContext applicationContext)
      throws BeansException {
    context = applicationContext;
  }

  public static ApplicationContext getContext() {
    if (context == null) {
      throw new RuntimeException("Spring not init");
    }
    return context;
  }


  public static Environment getEnvironment(){
    return getContext().getEnvironment();
  }


  public static String getApplicationName(){
    return getEnvironment().getProperty("spring.application.name");
  }

  public static String getActiveProfile(){
    final String[] activeProfiles = getEnvironment().getActiveProfiles();
    return activeProfiles.length > 0 ? activeProfiles[0] : null;
  }
}
