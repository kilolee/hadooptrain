package com.knightly.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * UserAgent测试类
 * Created by knightly on 2017/11/29.
 */
public class UserAgentTest {

    /**
     *单机本地完成浏览器访问分类统计
     * @throws Exception
     */
    @Test
    public void testReadFile () throws Exception{
        String path = "G:\\workspace-for-IntelliJ\\hadooptrain\\src\\test\\resources\\10000_access.log";

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));

        String line = "";
        int i=0;

        Map<String,Integer> browserMap = new HashMap<String, Integer>();

        UserAgentParser userAgentParser=new UserAgentParser();
        while (line != null){
            line = reader.readLine();//一次读入一行数据

            if (StringUtils.isNotBlank(line)){
                String source = line.substring(getCharacterPosition(line,"\"",7)+1);
                UserAgent agent = userAgentParser.parse(source);
                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String os = agent.getOs();
                String platform = agent.getPlatform();
                boolean isMobile = agent.isMobile();

                Integer browserValue = browserMap.get(browser);
                if (browserValue != null){
                    browserMap.put(browser,browserValue+1);
                }else {
                    browserMap.put(browser,1);
                }

                i++;
//                System.out.println(browser+" , "+engine+" , "+engineVersion+" , "+os+" , "+platform+" , "+isMobile);
            }
        }

        System.out.println("UserAgentTest.testReadFile,records count:"+ i);

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        for (Map.Entry<String,Integer> entry:browserMap.entrySet()) {
            System.out.println(entry.getKey()+" : "+entry.getValue());
        }
    }

    /**
     * 测试自定义方法
     */
    @Test
    public void testGetCharacterPosition(){
        String value = "117.35.88.11 - - [10/Nov/2016:00:01:02 +0800] \"GET /article/ajaxcourserecommends?id=124 HTTP/1.1\" 200 2345 \"www.imooc.com\" \"http://www.imooc.com/code/1852\" - \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36\" \"-\" 10.100.136.65:80 200 0.616 0.616\n";
        int index = getCharacterPosition(value,"\"",7);
        System.out.println(index);
    }

    /**
     * 获取指定字符串中指定标识的字符串出现的索引位置
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private  int getCharacterPosition(String value,String operator, int index){
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while (slashMatcher.find()){
            mIdx++;
            if (mIdx == index){
                break;
            }
        }
        return slashMatcher.start();
    }


    /**
     *测试用户代理解析器功能
     */
    @Test
    public void testUserAgentParser() {

        String source = "117.35.88.11 - - [10/Nov/2016:00:01:02 +0800] \"GET /article/ajaxcourserecommends?id=124 HTTP/1.1\" 200 2345 \"www.imooc.com\" \"http://www.imooc.com/code/1852\" - \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36\" \"-\" 10.100.136.65:80 200 0.616 0.616\n";

        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);

        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        boolean isMobile = agent.isMobile();

        System.out.println(browser+" , "+engine+" , "+engineVersion+" , "+os+" , "+platform+" , "+isMobile);

    }



}
