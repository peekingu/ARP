package nbt.wfw.fw.core.s;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import nbt.wfw.core.dao.IBasicDAO;
import nbt.wfw.core.dto.EntityReqDTO;
import nbt.wfw.core.dto.ReqResultDTO;
import nbt.wfw.core.dto.SingleBillReqDTO;
import nbt.wfw.core.ex.BasicException;
import nbt.wfw.core.ex.ExFactory;
import nbt.wfw.core.rpc.hdl.IBasicEntityHDL;
import nbt.wfw.core.rpc.hdl.IRpcSingleBillHDL;
import nbt.wfw.core.s.BasicService;
import nbt.wfw.core.sl.SpringServiceLocator;
import nbt.wfw.core.ui.util.TreeUIDatasUtil;
import nbt.wfw.core.ui.vo.TreeViewCfgVO;
import nbt.wfw.core.vo.BizBasicVO;
import nbt.wfw.fw.core.intf.IEntityRpcService;
import nbt.wfw.fw.core.util.DaoUtil;
import nbt.wfw.fw.core.util.VoUtil;
import nbt.wfw.fw.ex.FwExFactory;
import nbt.wfw.fw.id.IDFactory;
import nbt.wfw.fw.sql.util.SQLUtil;
import nbt.wfw.log.ILogger;
import nbt.wfw.log.mgr.LogFactory;
import nbt.wfw.sys.sm.dao.IUserDAO;
import nbt.wfw.util.data.JsonUtils;
import nbt.wfw.util.lang.DateUtil;
import nbt.wfw.util.lang.StringUtil;

public class EntityRpcService extends BasicService
  implements IEntityRpcService
{
  public ILogger _log = LogFactory.getLog(getClass());

  public String selectMapsAndCtByCondition(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;

    int ct = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }
      if (StringUtil.isBlank(dto.getParamCountWhere())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000208");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      ct = dao.countByCondition(dto.getParamCountWhere());

      datas = dao.selectMapsByCondition(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
      rdto.setExecTotalCount(ct);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectMapsByCondition(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      datas = dao.selectMapsByCondition(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectTreeViewByCondition(String jsonParam)
  {
    String retJsonParam = null;
    IBasicDAO dao = null;
    IUserDAO daoUsr = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;
    TreeViewCfgVO vo = null;
    String paramDaoNm = null; String paramStr = null; String paramClass = null; String paramVoStr = null;
    JSONObject jp = null;
    Object tmpObj = null;
    Class c = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }
      jp = JsonUtils.toJSONObject(jsonParam);

      tmpObj = jp.get("daoBeanName");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      paramDaoNm = tmpObj.toString();

      tmpObj = jp.get("paramString");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }
      paramStr = tmpObj.toString();

      tmpObj = jp.get("paramVoClass");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      paramClass = tmpObj.toString();
      c = Class.forName(paramClass);
      if (!TreeViewCfgVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000206");
      }

      tmpObj = jp.get("paramBizBasicVO");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000203");
      }
      paramVoStr = tmpObj.toString();

      vo = (TreeViewCfgVO)JsonUtils.jsonToBean(paramVoStr, c);

      boolean isQueryByVO = false;
      if (StringUtil.isNotBlank(vo.getTreeNodeClass())) {
        c = Class.forName(vo.getTreeNodeClass());
        if (!BizBasicVO.class.isAssignableFrom(c)) {
          throw FwExFactory.getInstance().getBizExAppend("ECM-NBT-FW-000207", "[Tree VO Class]");
        }
        BizBasicVO tvo = (BizBasicVO)c.newInstance();
        if ((tvo.getReferenceFields() != null) && (tvo.getReferenceFields().size() > 0))
        {
          isQueryByVO = true;
        }
      }

      if (isQueryByVO) {
        daoUsr = (IUserDAO)SpringServiceLocator.getInstance().getDAO("NBT-DAO-SysUser");
        String sql = SQLUtil.genVoSqlWithRefFields(c, paramStr);
        datas = daoUsr.selectMapsBySql(sql);
      } else {
        dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(paramDaoNm);
        datas = dao.selectMapsByCondition(paramStr);
      }

      datas = TreeUIDatasUtil.tansferTreeUIView(datas, vo);

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectTreeViewBySql(String jsonParam)
  {
    String retJsonParam = null;
    IBasicDAO dao = null;
    IUserDAO daoUsr = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;
    TreeViewCfgVO vo = null;
    String paramDaoNm = null; String paramStr = null; String paramClass = null; String paramVoStr = null;
    JSONObject jp = null;
    Object tmpObj = null;
    Class c = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }
      jp = JsonUtils.toJSONObject(jsonParam);

      tmpObj = jp.get("daoBeanName");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      paramDaoNm = tmpObj.toString();

      tmpObj = jp.get("paramString");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }
      paramStr = tmpObj.toString();

      tmpObj = jp.get("paramVoClass");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      paramClass = tmpObj.toString();
      c = Class.forName(paramClass);
      if (!TreeViewCfgVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000206");
      }

      tmpObj = jp.get("paramBizBasicVO");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000203");
      }
      paramVoStr = tmpObj.toString();

      vo = (TreeViewCfgVO)JsonUtils.jsonToBean(paramVoStr, c);

      boolean isQueryByVO = false;
      if (StringUtil.isNotBlank(vo.getTreeNodeClass())) {
        c = Class.forName(vo.getTreeNodeClass());
        if (!BizBasicVO.class.isAssignableFrom(c)) {
          throw FwExFactory.getInstance().getBizExAppend("ECM-NBT-FW-000207", "[Tree VO Class]");
        }
        BizBasicVO tvo = (BizBasicVO)c.newInstance();
        if ((tvo.getReferenceFields() != null) && (tvo.getReferenceFields().size() > 0))
        {
          isQueryByVO = true;
        }
      }

      if (isQueryByVO) {
        daoUsr = (IUserDAO)SpringServiceLocator.getInstance().getDAO("NBT-DAO-SysUser");
        String sql = SQLUtil.genVoSqlWithRefFields(c, paramStr);
        datas = daoUsr.selectMapsBySql(sql);
      } else {
        dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(paramDaoNm);
        datas = dao.selectMapsByCondition(paramStr);
      }

      datas = TreeUIDatasUtil.tansferTreeUIView(datas, vo);

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String deleteByPrimaryKey(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    int datas = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      datas = dao.deleteByPrimaryKey(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(datas));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String deleteByPrimaryKeyMulti(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();

    List vos = null;
    String pk = null;
    IBasicEntityHDL hdl = null;
    int ct = 0; int execRow = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }

      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      if (StringUtil.isBlank(dto.getParamVoClass())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      Class c = Class.forName(dto.getParamVoClass());
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      if (StringUtil.isNotBlank(dto.getParamEntityHdlKey())) {
        if (SpringServiceLocator.getInstance().containsBean(dto.getParamEntityHdlKey()))
          hdl = (IBasicEntityHDL)SpringServiceLocator.getInstance().getService(dto.getParamEntityHdlKey(), IBasicEntityHDL.class, IBasicEntityHDL.class);
        else {
          throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000209");
        }
      }

      vos = JsonUtils.jsonToList(dto.getParamString(), c);

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      if ((vos != null) && (vos.size() > 0)) {
        for (int i = 0; i < vos.size(); i++) {
          if (hdl != null) {
            hdl.beforeDeleteVO(dao, (BizBasicVO)vos.get(i));
          }
          pk = VoUtil.getPrimaryKey((BizBasicVO)vos.get(i));
          if (StringUtil.isBlank(pk)) {
            throw ExFactory.getInstance().getBizEx("ECM-NBT-FW-000102", "VO对象主键值为空!");
          }
          execRow = dao.deleteByPrimaryKey(pk);
          if (hdl != null) {
            hdl.afterDeleteVO(dao, (BizBasicVO)vos.get(i));
          }
          if (execRow == 1) {
            ct++;
          }
        }
      }

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(ct));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String insert(String jsonParam)
  {
    String retJsonParam = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    BizBasicVO datas = null;
    BizBasicVO vo = null;
    int ct = 0;
    String paramDaoNm = null; String paramClass = null; String paramVoStr = null;
    JSONObject jp = null;
    Object tmpObj = null;
    Class c = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }
      jp = JsonUtils.toJSONObject(jsonParam);

      tmpObj = jp.get("daoBeanName");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      paramDaoNm = tmpObj.toString();

      tmpObj = jp.get("paramVoClass");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      paramClass = tmpObj.toString();
      c = Class.forName(paramClass);
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      tmpObj = jp.get("paramBizBasicVO");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000203");
      }
      paramVoStr = tmpObj.toString();

      vo = (BizBasicVO)JsonUtils.jsonToBean(paramVoStr, c);
      if (StringUtil.isBlank(VoUtil.getPrimaryKey(vo))) {
        VoUtil.setPrimaryKey(vo, IDFactory.getInstance().getID());
      }
      VoUtil.setAttributeValue(vo, "dr", Integer.valueOf(0));

      VoUtil.setAttributeValue(vo, "ts", DateUtil.getDateTime());

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(paramDaoNm);

      ct = dao.insert(vo);

      datas = vo;

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectByPrimaryKey(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    Object datas = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      datas = dao.selectByPrimaryKey(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String updateByPrimaryKey(String jsonParam)
  {
    String retJsonParam = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    BizBasicVO datas = null;
    BizBasicVO vo = null;
    int ct = 0;
    String paramDaoNm = null; String paramClass = null; String paramVoStr = null;
    JSONObject jp = null;
    Object tmpObj = null;
    Class c = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }
      jp = JsonUtils.toJSONObject(jsonParam);

      tmpObj = jp.get("daoBeanName");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      paramDaoNm = tmpObj.toString();

      tmpObj = jp.get("paramVoClass");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      paramClass = tmpObj.toString();
      c = Class.forName(paramClass);
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      tmpObj = jp.get("paramBizBasicVO");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000203");
      }
      paramVoStr = tmpObj.toString();

      vo = (BizBasicVO)JsonUtils.jsonToBean(paramVoStr, c);
      if (StringUtil.isBlank(VoUtil.getPrimaryKey(vo))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000204");
      }
      VoUtil.setAttributeValue(vo, "dr", Integer.valueOf(0));
      VoUtil.setAttributeValue(vo, "ts", DateUtil.getDateTime());

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(paramDaoNm);

      ct = dao.updateByPrimaryKey(vo);

      datas = vo;

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String updateByStatus(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List td = null;

    List vos = null;
    BizBasicVO vo = null;
    String pk = null;
    IBasicEntityHDL hdl = null;
    int execRow = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }

      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      if (StringUtil.isBlank(dto.getParamVoClass())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      Class c = Class.forName(dto.getParamVoClass());
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      if (StringUtil.isNotBlank(dto.getParamEntityHdlKey())) {
        if (SpringServiceLocator.getInstance().containsBean(dto.getParamEntityHdlKey()))
          hdl = (IBasicEntityHDL)SpringServiceLocator.getInstance().getService(dto.getParamEntityHdlKey(), IBasicEntityHDL.class, IBasicEntityHDL.class);
        else {
          throw FwExFactory.getInstance().getBizExAppend("ECM-NBT-FW-000209", "(" + dto.getParamEntityHdlKey() + ")");
        }
      }

      vos = JsonUtils.jsonToList(dto.getParamString(), c);

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      if ((vos != null) && (vos.size() > 0)) {
        td = new ArrayList();
        for (int i = 0; i < vos.size(); i++) {
          vo = (BizBasicVO)vos.get(i);

          pk = VoUtil.getPrimaryKey(vo);
          if (vo.getStatus() == 2) {
            if (hdl != null) {
              hdl.beforeInsertVO(dao, vo);
            }
            if (StringUtil.isBlank(pk)) {
              VoUtil.setPrimaryKey(vo, IDFactory.getInstance().getID());
            }
            VoUtil.setAttributeValue(vo, "dr", Integer.valueOf(0));

            VoUtil.setAttributeValue(vo, "ts", DateUtil.getDateTime());

            execRow = dao.insert(vo);

            if (hdl != null) {
              hdl.afterInsertVO(dao, vo);
            }
            vo.setStatus(0);
            td.add(vo);
          } else if (vo.getStatus() == 1) {
            if (hdl != null) {
              hdl.beforeUpdateVO(dao, vo);
            }
            VoUtil.setAttributeValue(vo, "dr", Integer.valueOf(0));
            VoUtil.setAttributeValue(vo, "ts", DateUtil.getDateTime());

            execRow = dao.updateByPrimaryKey(vo);
            if (hdl != null) {
              hdl.afterUpdateVO(dao, vo);
            }
            vo.setStatus(0);
            td.add(vo);
          } else if (vo.getStatus() == 3) {
            if (hdl != null) {
              hdl.beforeDeleteVO(dao, vo);
            }
            if (StringUtil.isBlank(pk)) {
              throw ExFactory.getInstance().getBizEx("ECM-NBT-FW-000102", "VO对象主键值为空!(" + vo.getClass().getName() + ")");
            }
            execRow = dao.deleteByPrimaryKey(pk);
            if (hdl != null) {
              hdl.afterDeleteVO(dao, vo);
            }
          }
        }

      }

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(td);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectByCondition(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      datas = dao.selectByCondition(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String countByCondition(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    int datas = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      datas = dao.countByCondition(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(datas));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String deleteByCondition(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    int datas = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      datas = dao.deleteByCondition(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(datas));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String discardByCondition(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    int datas = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      datas = dao.discardByCondition(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(datas));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String discardByPrimaryKey(String jsonParam)
  {
    String retJsonParam = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    BizBasicVO vo = null;
    int datas = 0;
    String paramDaoNm = null; String paramClass = null; String paramVoStr = null;
    JSONObject jp = null;
    Object tmpObj = null;
    Class c = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }
      jp = JsonUtils.toJSONObject(jsonParam);

      tmpObj = jp.get("daoBeanName");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }
      paramDaoNm = tmpObj.toString();

      tmpObj = jp.get("paramVoClass");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      paramClass = tmpObj.toString();
      c = Class.forName(paramClass);
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      tmpObj = jp.get("paramBizBasicVO");
      if ((tmpObj == null) || (StringUtil.isBlank(tmpObj.toString()))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000203");
      }
      paramVoStr = tmpObj.toString();

      vo = (BizBasicVO)JsonUtils.jsonToBean(paramVoStr, c);

      if (StringUtil.isBlank(VoUtil.getPrimaryKey(vo))) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000204");
      }

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(paramDaoNm);

      VoUtil.setAttributeValue(vo, "ts", DateUtil.getDateTime());
      VoUtil.setAttributeValue(vo, "dr", Integer.valueOf(1));
      datas = dao.discardByPrimaryKey(vo);

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(datas));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String discardByPrimaryKeyMulti(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IBasicDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();

    List vos = null;
    String pk = null;
    IBasicEntityHDL hdl = null;
    int ct = 0; int execRow = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getDaoBeanName())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000201");
      }

      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      if (StringUtil.isBlank(dto.getParamVoClass())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      Class c = Class.forName(dto.getParamVoClass());
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      if (StringUtil.isNotBlank(dto.getParamEntityHdlKey())) {
        if (SpringServiceLocator.getInstance().containsBean(dto.getParamEntityHdlKey()))
          hdl = (IBasicEntityHDL)SpringServiceLocator.getInstance().getService(dto.getParamEntityHdlKey(), IBasicEntityHDL.class, IBasicEntityHDL.class);
        else {
          throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000209");
        }
      }

      vos = JsonUtils.jsonToList(dto.getParamString(), c);

      dao = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getDaoBeanName());

      if ((vos != null) && (vos.size() > 0)) {
        for (int i = 0; i < vos.size(); i++) {
          if (hdl != null) {
            hdl.beforeDiscardVO(dao, (BizBasicVO)vos.get(i));
          }
          VoUtil.setAttributeValue((BizBasicVO)vos.get(i), "ts", DateUtil.getDateTime());
          VoUtil.setAttributeValue((BizBasicVO)vos.get(i), "dr", Integer.valueOf(1));
          pk = VoUtil.getPrimaryKey((BizBasicVO)vos.get(i));
          if (StringUtil.isBlank(pk)) {
            throw ExFactory.getInstance().getBizEx("ECM-NBT-FW-000102", "VO对象主键值为空!");
          }
          execRow = dao.discardByPrimaryKey((BizBasicVO)vos.get(i));
          if (hdl != null) {
            hdl.afterDiscardVO(dao, (BizBasicVO)vos.get(i));
          }
          if (execRow == 1) {
            ct++;
          }
        }
      }

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(ct));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String countBySql(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IUserDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    int datas = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IUserDAO)SpringServiceLocator.getInstance().getDAO("NBT-DAO-SysUser");

      this._log.debug("[Buddy-SQL] -CT- " + dto.getParamString());
      datas = dao.countBySql(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(Integer.valueOf(datas));
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectMapsBySql(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IUserDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      dao = (IUserDAO)SpringServiceLocator.getInstance().getDAO("NBT-DAO-SysUser");

      this._log.debug("[Buddy-SQL] -QRY- " + dto.getParamString());
      datas = dao.selectMapsBySql(dto.getParamString());

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String updateBillByStatus(String jsonParam)
  {
    String retJsonParam = null;
    ReqResultDTO rdto = new ReqResultDTO();
    BizBasicVO td = null;

    SingleBillReqDTO dto = null;
    Class headClazz = null; Class bodyClazz = null;
    IRpcSingleBillHDL hdl = null;
    IBasicDAO daoHead = null; IBasicDAO daoBody = null;
    BizBasicVO hvo = null;
    List bvos = null;

    String pk = null;
    int execRow = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (SingleBillReqDTO)JsonUtils.jsonToBean(jsonParam, SingleBillReqDTO.class);

      if (StringUtil.isBlank(dto.getHeadDaoKN())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000210");
      }
      if (StringUtil.isBlank(dto.getBodyDaoKN())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000211");
      }

      if (StringUtil.isBlank(dto.getHeadVoClazz())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000212");
      }
      headClazz = Class.forName(dto.getHeadVoClazz());
      if (!BizBasicVO.class.isAssignableFrom(headClazz)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }
      if (StringUtil.isBlank(dto.getBodyVoClazz())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000213");
      }
      bodyClazz = Class.forName(dto.getBodyVoClazz());
      if (!BizBasicVO.class.isAssignableFrom(headClazz)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      if (StringUtil.isNotBlank(dto.getBillHdlKey())) {
        if (SpringServiceLocator.getInstance().containsBean(dto.getBillHdlKey()))
          hdl = (IRpcSingleBillHDL)SpringServiceLocator.getInstance().getService(dto.getBillHdlKey(), IRpcSingleBillHDL.class, IRpcSingleBillHDL.class);
        else {
          throw FwExFactory.getInstance().getBizExAppend("ECM-NBT-FW-000209", "(" + dto.getBillHdlKey() + ")");
        }

      }

      if (StringUtil.isBlank(dto.getHeadVoDatas())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000214");
      }
      hvo = (BizBasicVO)JsonUtils.jsonToBean(dto.getHeadVoDatas(), headClazz);
      if (StringUtil.isBlank(dto.getBodyVoDatas())) {
        if (dto.isBodyNeedSwitch())
          throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000215");
      }
      else {
        bvos = JsonUtils.jsonToList(dto.getBodyVoDatas(), bodyClazz);
      }

      daoHead = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getHeadDaoKN());
      daoBody = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getBodyDaoKN());

      if (hvo.getStatus() == 2)
      {
        if (hdl != null) {
          hdl.beforeInsertHeadVO(daoHead, hvo, bvos);
        }
        pk = VoUtil.getPrimaryKey(hvo);
        if (StringUtil.isBlank(pk)) {
          VoUtil.setPrimaryKey(hvo, IDFactory.getInstance().getID());
        }
        VoUtil.setAttributeValue(hvo, "dr", Integer.valueOf(0));
        VoUtil.setAttributeValue(hvo, "ts", DateUtil.getDateTime());
        execRow = daoHead.insert(hvo);

        if ((bvos != null) && (bvos.size() > 0)) {
          execRow = DaoUtil.insertVos(daoBody, hvo, bvos, hdl);
        }
        if (hdl != null) {
          hdl.afterInsertHeadVO(daoHead, hvo, bvos);
        }
        hvo.setStatus(0);
        td = hvo;
      } else if (hvo.getStatus() == 1)
      {
        if (hdl != null) {
          hdl.beforeUpdateHeadVO(daoHead, hvo, bvos);
        }
        VoUtil.setAttributeValue(hvo, "dr", Integer.valueOf(0));
        VoUtil.setAttributeValue(hvo, "ts", DateUtil.getDateTime());

        execRow = daoHead.updateByPrimaryKey(hvo);

        if ((bvos != null) && (bvos.size() > 0)) {
          DaoUtil.updateVos(daoBody, hvo, bvos, hdl);
        }
        if (hdl != null) {
          hdl.afterUpdateHeadVO(daoHead, hvo, bvos);
        }
        hvo.setStatus(0);
        td = hvo;
      } else if (hvo.getStatus() == 3)
      {
        if (hdl != null) {
          hdl.beforeDeleteHeadVO(daoHead, hvo, bvos);
        }
        if (StringUtil.isBlank(pk)) {
          throw ExFactory.getInstance().getBizEx("ECM-NBT-FW-000102", "VO对象主键值为空!(" + hvo.getClass().getName() + ")");
        }
        execRow = DaoUtil.deleteBillBodys(daoBody, hvo, bodyClazz);

        execRow = daoHead.deleteByPrimaryKey(pk);

        if (hdl != null)
          hdl.afterDeleteHeadVO(daoHead, hvo, bvos);
      }
      else {
        throw FwExFactory.getInstance().getBizExAppend("ECM-NBT-FW-000216", "[" + hvo.getStatus() + "]");
      }

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(td);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String deleteBill(String jsonParam)
  {
    String retJsonParam = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List td = new ArrayList();

    SingleBillReqDTO dto = null;
    Class headClazz = null; Class bodyClazz = null;
    IRpcSingleBillHDL hdl = null;
    IBasicDAO daoHead = null; IBasicDAO daoBody = null;
    BizBasicVO hvo = null;
    List bvos = null;

    String pk = null;
    int execRow = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (SingleBillReqDTO)JsonUtils.jsonToBean(jsonParam, SingleBillReqDTO.class);

      if (StringUtil.isBlank(dto.getHeadDaoKN())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000210");
      }
      if (StringUtil.isBlank(dto.getBodyDaoKN())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000211");
      }

      if (StringUtil.isBlank(dto.getHeadVoClazz())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000212");
      }
      headClazz = Class.forName(dto.getHeadVoClazz());
      if (!BizBasicVO.class.isAssignableFrom(headClazz)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }
      if (StringUtil.isBlank(dto.getBodyVoClazz())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000213");
      }
      bodyClazz = Class.forName(dto.getBodyVoClazz());
      if (!BizBasicVO.class.isAssignableFrom(headClazz)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      if (StringUtil.isNotBlank(dto.getBillHdlKey())) {
        if (SpringServiceLocator.getInstance().containsBean(dto.getBillHdlKey()))
          hdl = (IRpcSingleBillHDL)SpringServiceLocator.getInstance().getService(dto.getBillHdlKey(), IRpcSingleBillHDL.class, IRpcSingleBillHDL.class);
        else {
          throw FwExFactory.getInstance().getBizExAppend("ECM-NBT-FW-000209", "(" + dto.getBillHdlKey() + ")");
        }

      }

      if (StringUtil.isBlank(dto.getHeadVoDatas())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000214");
      }
      hvo = (BizBasicVO)JsonUtils.jsonToBean(dto.getHeadVoDatas(), headClazz);

      daoHead = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getHeadDaoKN());
      daoBody = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getBodyDaoKN());

      if (hdl != null) {
        hdl.beforeDeleteHeadVO(daoHead, hvo, bvos);
      }
      if (StringUtil.isBlank(pk)) {
        throw ExFactory.getInstance().getBizEx("ECM-NBT-FW-000102", "VO对象主键值为空!(" + hvo.getClass().getName() + ")");
      }
      execRow = DaoUtil.deleteBillBodys(daoBody, hvo, bodyClazz);

      execRow = daoHead.deleteByPrimaryKey(pk);

      if (hdl != null) {
        hdl.afterDeleteHeadVO(daoHead, hvo, bvos);
      }

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(td);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String deleteBillMulti(String jsonParam)
  {
    String retJsonParam = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List td = new ArrayList();

    SingleBillReqDTO dto = null;
    Class headClazz = null; Class bodyClazz = null;
    IRpcSingleBillHDL hdl = null;
    IBasicDAO daoHead = null; IBasicDAO daoBody = null;
    List hvos = null;
    List bvos = null;
    BizBasicVO hvo = null;

    String pk = null;
    int execRow = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (SingleBillReqDTO)JsonUtils.jsonToBean(jsonParam, SingleBillReqDTO.class);

      if (StringUtil.isBlank(dto.getHeadDaoKN())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000210");
      }
      if (StringUtil.isBlank(dto.getBodyDaoKN())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000211");
      }

      if (StringUtil.isBlank(dto.getHeadVoClazz())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000212");
      }
      headClazz = Class.forName(dto.getHeadVoClazz());
      if (!BizBasicVO.class.isAssignableFrom(headClazz)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }
      if (StringUtil.isBlank(dto.getBodyVoClazz())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000213");
      }
      bodyClazz = Class.forName(dto.getBodyVoClazz());
      if (!BizBasicVO.class.isAssignableFrom(headClazz)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      if (StringUtil.isNotBlank(dto.getBillHdlKey())) {
        if (SpringServiceLocator.getInstance().containsBean(dto.getBillHdlKey()))
          hdl = (IRpcSingleBillHDL)SpringServiceLocator.getInstance().getService(dto.getBillHdlKey(), IRpcSingleBillHDL.class, IRpcSingleBillHDL.class);
        else {
          throw FwExFactory.getInstance().getBizExAppend("ECM-NBT-FW-000209", "(" + dto.getBillHdlKey() + ")");
        }

      }

      if (StringUtil.isBlank(dto.getHeadVoDatas())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000214");
      }
      hvos = JsonUtils.jsonToList(dto.getHeadVoDatas(), headClazz);

      daoHead = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getHeadDaoKN());
      daoBody = (IBasicDAO)SpringServiceLocator.getInstance().getDAO(dto.getBodyDaoKN());

      if ((hvos != null) && (hvos.size() > 0)) {
        for (int i = 0; i < hvos.size(); i++) {
          hvo = (BizBasicVO)hvos.get(i);
          pk = VoUtil.getPrimaryKey(hvo);
          if (hdl != null) {
            hdl.beforeDeleteHeadVO(daoHead, hvo, bvos);
          }
          if (StringUtil.isBlank(pk)) {
            throw ExFactory.getInstance().getBizEx("ECM-NBT-FW-000102", "VO对象主键值为空!(" + hvo.getClass().getName() + ")");
          }
          execRow = DaoUtil.deleteBillBodys(daoBody, hvo, bodyClazz);

          execRow = daoHead.deleteByPrimaryKey(pk);

          if (hdl != null) {
            hdl.afterDeleteHeadVO(daoHead, hvo, bvos);
          }
        }
      }

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(td);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectMapsByVO(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IUserDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;
    String sql = null;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      if (StringUtil.isBlank(dto.getParamVoClass())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      Class c = Class.forName(dto.getParamVoClass());
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      dao = (IUserDAO)SpringServiceLocator.getInstance().getDAO("NBT-DAO-SysUser");

      sql = SQLUtil.genVoSqlWithRefFields(c, dto.getParamString());
      this._log.debug("[Buddy-SQL] -QRY- " + sql);

      datas = dao.selectMapsBySql(sql);

      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }

  public String selectMapsAndCtByVO(String jsonParam)
  {
    String retJsonParam = null;
    EntityReqDTO dto = null;
    IUserDAO dao = null;
    ReqResultDTO rdto = new ReqResultDTO();
    List datas = null;
    String sql = null;
    int ct = 0;
    try
    {
      if (StringUtil.isBlank(jsonParam)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000200");
      }

      dto = (EntityReqDTO)JsonUtils.jsonToBean(jsonParam, EntityReqDTO.class);

      if (StringUtil.isBlank(dto.getParamString())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000202");
      }

      if (StringUtil.isBlank(dto.getParamVoClass())) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000205");
      }
      Class c = Class.forName(dto.getParamVoClass());
      if (!BizBasicVO.class.isAssignableFrom(c)) {
        throw FwExFactory.getInstance().getFwEx("ECM-NBT-FW-000207");
      }

      dao = (IUserDAO)SpringServiceLocator.getInstance().getDAO("NBT-DAO-SysUser");
      
      sql = SQLUtil.genVoSqlCtWithRefFields(c, dto.getParamCountWhere());
      this._log.debug("[Buddy-SQL] -CT- " + sql);
      long time1 = System.currentTimeMillis();
      ct = dao.countBySql(sql);
      long end = System.currentTimeMillis() - time1;
      
      System.out.println("consume time1**********************:"+end);
      
      sql = SQLUtil.genVoSqlWithRefFields(c, dto.getParamString());
      this._log.debug("[Buddy-SQL] -QRY- " + sql);
      
      time1 = System.currentTimeMillis();
      
      datas = dao.selectMapsBySql(sql);
      end = System.currentTimeMillis() - time1;
      System.out.println("consume time2**********************:"+end);
      
      rdto.setExecResult(true);
      rdto.setExecMsg("");
      rdto.setExecDatas(datas);
      rdto.setExecTotalCount(ct);
    } catch (BasicException bex) {
      rdto.setExecResult(false);
      rdto.setExecErrCode(bex.getExErrorCode());
      rdto.setExecMsg(bex.getMessage());
    } catch (Exception e) {
      if (StringUtil.isBlank(e.getMessage())) {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[NULL]");
      } else {
        rdto.setExecResult(false);
        rdto.setExecErrCode("ECM-NBT-FW-000605");
        rdto.setExecMsg("后台服务执行错误.[" + e.getMessage() + "]");
      }
    }

    retJsonParam = JsonUtils.toJsonStr(rdto);
    return retJsonParam;
  }
}