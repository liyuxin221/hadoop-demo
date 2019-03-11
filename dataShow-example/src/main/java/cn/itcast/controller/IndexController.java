package cn.itcast.controller;

import cn.itcast.service.AvgNumService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author: Liyuxin wechat:13011800146.
 * @Title: ShowIndex
 * @ProjectName hadoop-parent
 * @date: 2019/3/10 16:44
 * @description:
 */
@Controller
public class IndexController {

	@Autowired
	private AvgNumService service;

	@RequestMapping("/index")
	public String showIndex() {
		return "index";
	}

	@RequestMapping(value = "/avgPvNum", produces = "application/json;charset=UTF-8")
	@ResponseBody
	public String getAvgNum() {
		return this.service.getAvgNumByDates("20130919", "20130925");
	}
}
