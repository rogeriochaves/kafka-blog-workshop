package com.example.kafkablog;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Controller
public class HomeController {
    @GetMapping({"/"})
    public String home(Model model) {
        model.addAttribute("post", new Post());

        return "home";
    }

    @PostMapping({"/posts/save"})
    public String save(@ModelAttribute Post post) {
        UUID uuid = UUID.randomUUID();
        post.setId(uuid.toString());
        System.out.print(post);

        return "redirect:/";
    }
}
