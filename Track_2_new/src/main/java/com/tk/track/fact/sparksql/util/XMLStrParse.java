package com.tk.track.fact.sparksql.util;

//import org.dom4j.Document;
//import org.dom4j.DocumentException;
//import org.dom4j.DocumentHelper;
//import org.dom4j.Node;


import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

/**
 * Created by itw_chenhao01 on 2017/2/20.
 */
public class XMLStrParse {

    public static String getSingleNodeByTagName(String domXML,String xpath){
//        Document dom=null;
//        try {
//            dom=DocumentHelper.parseText(domXML);
//        } catch (DocumentException e) {
//            e.printStackTrace();
//        }
//       Node node= dom.selectSingleNode(xpath);
//        if(node!=null){
//            return node.getText();
//        }else
            return "";
    }

    public static String getSingleTagNameAttri(String domXML,String tagName,String attributeName){
        String result="";
        if(StringUtils.isNotBlank(domXML)&&StringUtils.isNotBlank(tagName)&&
                StringUtils.isNotBlank(attributeName)){
            StringReader sr = new StringReader(domXML);
            InputSource is = new InputSource(sr);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            DocumentBuilder builder= null;

            try {
                builder = factory.newDocumentBuilder();
                Document doc = builder.parse(is);
                result= getAttributeValueFromDom(doc,tagName,attributeName);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }catch (SAXException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            result="";
        }
        return result;
    }
    public static String getSingleByTagName(String domXML,String tagName){
        String result="";
        if(StringUtils.isNotBlank(domXML)&&StringUtils.isNotBlank(tagName)){
            StringReader sr = new StringReader(domXML);
            InputSource is = new InputSource(sr);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder= null;
            try {
                builder = factory.newDocumentBuilder();
                Document doc = builder.parse(is);
                result= getNodeValueFromDom(doc, tagName);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }catch (SAXException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            result="";
        }
        return result;
    }

    /**
     * 根据tagName,attributeName从xml中取得取得相应Node属性值
     * 可能有多个tagName定义的Node，取第一个
     * @param dom
     * @param tagName
     * @attributeName
     * @return Attribute;
     */
    public static String getAttributeValueFromDom(Document dom,String tagName,String attributeName)
    {
        String result = "";
        if (dom != null && tagName != null && !tagName.trim().equals("")
                && attributeName != null && !attributeName.trim().equals(""))
        {
            NodeList nodeList = null;
            Node node = null;
            nodeList = dom.getElementsByTagName(tagName.trim());
            if (nodeList !=null && nodeList.getLength() > 0)
            {
                /**
                 * xml中可能有多个tagName定义的Node，取第一个
                 */
                node = nodeList.item(0);
                if (node != null)
                {
                    result = ((Element)node).getAttribute(attributeName);
                    result = result == null?"":result.trim();
                }
            }
        }
        return result;
    }
    /**
     * 根据tagName从xml中取得取得相应Node值
     * @param dom
     * @param tagName
     * @return
     */
    public static String getNodeValueFromDom(Document dom,String tagName)
    {
        String result = "";
        try
        {
            if (dom != null && tagName != null && !tagName.trim().equals(""))
            {
                NodeList nodeList = null;
                Node node = null;
                nodeList = dom.getElementsByTagName(tagName.trim());
                if (nodeList !=null && nodeList.getLength() > 0)
                {
                    /**
                     * xml中可能有多个tagName定义的Node，取第一个
                     */
                    node = nodeList.item(0);
                    if (node != null)
                    {
                        node = node.getFirstChild();
                        if (node != null)
                            result = node.getNodeValue();
                        result = result == null?"":result.trim();
                    }
                }
            }
        }catch(Exception e)
        {
            System.out.println(" getNodeValueFromDom 出错 = " + e.toString());
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        String str="<?xml version=\"1.0\" encoding=\"GBK\"?>\n" +
                "<消息 关键字=\"\" 客户端IP=\"106.121.4.179\" 客户端编号=\"53686\" 操作员编号=\"\" 来源=\"网上投保\" 活动编号=\"\" 版本=\"1.0\"><消息内容 功能=\"投保\"><保单信息 保单介质=\"电子保单\" 寄送方式=\"电子\" 投保地区=\"00000001000110000000\" 提交时间=\"2017-02-16 18:42:56\" 支付方式=\"67\" 支付银行=\"广州银联实时\" 类型=\"投保单\" 订单号=\"2596452\" 支付显示编号=\"\" 是否预售=\"\" 流水号=\"\" 保单号码=\"\" 自动垫交保费=\"\" 红利领取方式=\"\" 合作方流水号=\"\"><保险信息 缴费方式=\"\" 险种名称=\"泰康e生健康重大疾病保障计划\" 险种编号=\"150\" 保险计划编号=\"H0006\" 流程编号=\"1004\" 套餐编号=\"I0011_MOBILE_PLAN\" 投保对象=\"M\"><保险期间 从=\"2017-02-17 00:00:00\" 至=\"9999-12-31 00:00:00\"/><出入境/><行程/><旅行方式/></保险信息><投保人 类型=\"个人\"><个人 体重=\"55\" 出生日期=\"1990-10-15\" 国籍=\"中国\" 地址=\"难受难受难受难受死你\" 姓名=\"中段\" 年收入=\"\" 性别=\"男\" 电子邮件=\"13311472526@163.com\" 移动电话=\"13311472526\" 职业=\"机关团体/企事业单位/公司内勤人员/行政内勤/销售外勤人员\" 职业代码=\"2201001\" 联系电话=\"\" 证件名称=\"01\" 证件编号=\"110100199010153951\" 身份证有效期=\"\" 身高=\"165\" 邮编=\"\" 预产期=\"\" 原证件名称=\"\"/></投保人><被保险人列表 总保费=\"12700.0\" 数量=\"1\" 真实保费=\"12700.00\" 类型=\"个人\" 邮寄费=\"0\"><被保险人 与投保人关系=\"本人\" 体重=\"55\" 保费=\"12700.0\" 出生日期=\"1990-10-15\" 国籍=\"中国\" 声明保额=\"0.00\" 声明险种=\"\" 备注=\"\" 姓名=\"中段\" 家庭角色=\"\" 年收入=\"\" 序号=\"\" 性别=\"男\" 签证国家=\"\" 职业=\"机关团体/企事业单位/公司内勤人员/行政内勤/销售外勤人员\" 职业代码=\"2201001\" 英文名=\"\" 证件名称=\"01\" 证件编号=\"110100199010153951\" 身份证有效期=\"\" 识别码=\"\" 身高=\"165\" 预产期=\"\" 被保险人手机号=\"\"><购买产品 名称=\"泰康e生健康重大疾病保障计划\" 编号=\"150\"><子险 份数=\"1\" 保费=\"12700.0\" 保险金额=\"50000\" 名称=\"泰康e康B款终身重大疾病保险[全部责任]\" 编号=\"1501\" 保险责任代码=\"141001\" 基础免赔额=\"\" 主险=\"Y\" 缴费期间=\"SP\" 缴费方式=\"SP\" 保险期间=\"999999\" 领取方式=\"\" 领取年龄=\"\"/></购买产品><受益人列表><身故受益人 姓名=\"法定继承人\"/></受益人列表><紧急联系人 类型=\"\"><个人 关系=\"\" 出生日期=\"\" 姓名=\"\" 性别=\"\" 手机=\"\" 电子邮箱=\"\" 联系电话=\"\" 证件名称=\"\" 证件编号=\"\" 通信地址=\"\" 邮编=\"\"/></紧急联系人><告知列表 版本=\"1.0\"><告知 描述=\" \"><编号 answer=\"否\" id=\"bfd7c6b7-728a-49c4-b7b9-fe7cdf02bbec\"/><编号 answer=\"否\" id=\"0eff66c9-b2e8-43c4-9e05-d10bd3d37478\"/><编号 answer=\"否\" id=\"98a9c9b4-12bd-4483-8962-21817904e2d8\"/><编号 answer=\"否\" id=\"9ebc870c-7371-4186-8363-745ce38c2e33\"/><编号 answer=\"否\" id=\"13931b73-f410-40d5-9737-32fa2c2ffa8f\"/><编号 answer=\"否\" id=\"638eedd6-610a-4580-a79c-cc25962de8da\"/><编号 answer=\"否\" id=\"5461d5be-5eca-4acb-a230-23769d307ab8\"/></告知></告知列表></被保险人></被保险人列表><其他信息><代理人 编号=\"\" 星级=\"\"/><为他人投保 值=\"\"/><在线服务密码 值=\"\"/><注册用户编号 值=\"10220540\"/><投保单编号 值=\"1902220\"/><是否免登录 值=\"N\"/></其他信息><特殊信息><保健医生 ID=\"HMOCHECKUPDOCTOR\"/><门诊全科医生 ID=\"HMOCLINICDOCTOR\"/><指定医院 ID=\"HMOHOSPITAL\"/><种子编号 ID=\"REFERID\">8321531627858657srlhmkvyvXLr</种子编号></特殊信息><团购信息 团购代码=\"\" 团购序号=\"\" 团购认证码=\"\"/><扩展支付信息列表><支付信息 支付方式名称=\"\" 支付方式=\"\" 支付面额=\"\" 订单号=\"\"/></扩展支付信息列表><银行代扣交费信息 授权金额=\"12700.0\" 账号=\"5588422684469863\" 账户名=\"中段\" 银联代码=\"102\" 银行代码=\"\" 银行编号=\"ICBC\" 交费账户银行=\"工商银行\"/></保单信息></消息内容></消息>";
        System.out.println(str);
//        String result=getSingleByTagNameAttri(str,"种子编号","ID");
        String result=getSingleByTagName(str, "种子编号");
        System.out.println("ssss:"+result);
    }
}
