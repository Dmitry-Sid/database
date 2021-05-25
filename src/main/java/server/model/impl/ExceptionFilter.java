package server.model.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import java.io.IOException;

public class ExceptionFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(ExceptionFilter.class);

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        try {
            filterChain.doFilter(servletRequest, servletResponse);
        } catch (Throwable e) {
            log.error("Error during the request", e);
            throw e;
        }
    }
}
