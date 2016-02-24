package org.bde;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class ResultController {

	@RequestMapping("/result")
	String result(Model model, @RequestParam(value = "id") String id, @RequestParam(value = "artist") String artist, @RequestParam(value = "title") String title) {
		
		ArrayList<List<String>> rows = RestLet.getScore(id);
		ArrayList<String> header = new ArrayList<String>();

		header.add("Künstler");
		header.add("Titel");
		header.add("Score-Wert");

		model.addAttribute("header", header);
		model.addAttribute("rows", rows);
		model.addAttribute("artist", artist);
		model.addAttribute("title", title);

		return "result";
	}
}
