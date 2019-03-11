package cn.itcast.mapper;

import cn.itcast.pojo.TAvgpvNum;
import cn.itcast.pojo.TAvgpvNumExample;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface TAvgpvNumMapper {
	long countByExample(TAvgpvNumExample example);

	int deleteByExample(TAvgpvNumExample example);

	int insert(TAvgpvNum record);

	int insertSelective(TAvgpvNum record);

	List<TAvgpvNum> selectByExample(TAvgpvNumExample example);

	int updateByExampleSelective(@Param("record") TAvgpvNum record, @Param("example") TAvgpvNumExample example);

	int updateByExample(@Param("record") TAvgpvNum record, @Param("example") TAvgpvNumExample example);
}