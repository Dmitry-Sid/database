package server.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping(value = "/", produces = "text/plain;charset=UTF-8")
public class RedirectController {

    @GetMapping
    public String redirect() {
        return "redirect:/tablesManager";
    }
}
