package com.example.kafkablog;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Controller
public class HomeController {
    @GetMapping({"/"})
    public String home(Model model) {
        model.addAttribute("post", new Post());

        List<Post> posts = PostsStream.getInstance().findAll();
        model.addAttribute("posts", posts);

        return "home";
    }

    @PostMapping({"/posts/save"})
    public String save(@ModelAttribute Post post) {
        UUID uuid = UUID.randomUUID();
        post.setId(uuid.toString());
        PostsStream.getInstance().produce(post);

        return "redirect:/";
    }
}
