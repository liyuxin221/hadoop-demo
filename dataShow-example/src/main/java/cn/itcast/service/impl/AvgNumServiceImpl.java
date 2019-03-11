package cn.itcast.service.impl;

import cn.itcast.mapper.TAvgpvNumMapper;
import cn.itcast.pojo.AvgToPageBean;
import cn.itcast.pojo.TAvgpvNum;
import cn.itcast.pojo.TAvgpvNumExample;
import cn.itcast.service.AvgNumService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Allen Woon
 */
@Service
public class AvgNumServiceImpl implements AvgNumService {

    @Autowired
    private TAvgpvNumMapper mapper;

    @Override
    public String getAvgNumByDates(String startTime, String endTime) {
        TAvgpvNumExample example = new TAvgpvNumExample();
        TAvgpvNumExample.Criteria criteria = example.createCriteria();
        criteria.andDatestrBetween(startTime,endTime);//封装查询条件  根据日期范围查询
        //指定查询排序问题
        example.setOrderByClause("`dateStr` ASC");
        //根据条件查询返回的结果集合
        List<TAvgpvNum> lists = mapper.selectByExample(example);
        // 数组大小
        int size = 7;

        String[] dates = new String[size];
        double[] datas = new double[size];

        int i = 0;
        for (TAvgpvNum tAvgpvNum : lists) {
            dates[i]=tAvgpvNum.getDatestr();
            datas[i]=tAvgpvNum.getAvgpvnum().doubleValue();
            i++;
        }

        AvgToPageBean bean = new AvgToPageBean();
        bean.setDates(dates);
        bean.setData(datas);
        ObjectMapper om = new ObjectMapper();
        String beanJson= null;
        try {
            beanJson = om.writeValueAsString(bean);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return beanJson;
    }
}
