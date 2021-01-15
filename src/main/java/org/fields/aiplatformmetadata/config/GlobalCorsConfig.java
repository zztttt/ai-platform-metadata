package org.fields.aiplatformmetadata.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@Configuration
public class GlobalCorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")    // 允许跨域访问的路径
                .allowedOrigins("*")    // 允许跨域访问的源
                .allowedMethods("POST", "GET", "PUT", "DELETE")    // 允许请求方法
                .maxAge(36000)    // 预检间隔时间
                .allowedHeaders("*")  // 允许头部设置
                .exposedHeaders("Authorization")
                .allowCredentials(true);    // 是否发送cookie
    }
}
