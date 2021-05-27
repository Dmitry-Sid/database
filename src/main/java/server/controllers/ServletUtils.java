package server.controllers;

import javax.servlet.http.HttpServletRequest;

public class ServletUtils {

    public static void makeError(HttpServletRequest request, String message) {
        request.setAttribute("error", message);
    }

}
