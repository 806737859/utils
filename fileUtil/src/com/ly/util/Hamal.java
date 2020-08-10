package com.ly.util;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 搬运工
 * 将dbLink访问方式替换成用户名访问方式
 * eg : tableName@dbLink ——> userName.tableName
 * 注：没有对dblink换行的奇葩操作做处理，所以要确保tableName和dbLink在同一行，否则无法转换
 * @Author:         aiwusheng
 * @CreateDate:     2020/8/6 10:57
 */
public class Hamal {

    private static final Logger log = Logger.getLogger(Hamal.class.getName());

    public static void main(String[] args) {
        HashMap<String,String> map = new HashMap<>();
        //核心数据
        map.put("hxsj","tzjw_hxsj");
        //系统管理
        map.put("xtgl","tzjw_xtgl");
        //培养方案
        map.put("pyfa","tzjw_pyfa");

        //转换的路径
        List<String> filepathList = new ArrayList<>();
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-arrange-svc\\ly-cea-arrange-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-course-plan-svc\\ly-cea-course-plan-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-cultivation-plan-svc\\ly-cea-cultivation-plan-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-elective-cache-svc\\ly-cea-elective-cache-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-elective-svc\\ly-cea-elective-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-exam-affairs-svc\\ly-cea-exam-affairs-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-score-svc\\ly-cea-score-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-teaching-assessment-svc\\ly-cea-teaching-assessment-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-teaching-material-svc\\ly-cea-teaching-material-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-workload-svc\\ly-cea-workload-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cloud-auth-svc\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-edu-core-svc\\ly-edu-core-server\\src\\main\\resources\\mapper");
//        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-sm-student-status-svc\\ly-sm-student-status-server\\src\\main\\resources\\mapper");
        filepathList.add("E:\\idea\\idea_work\\lycea-dev\\ly-cea-graduation-svc\\ly-cea-graduation-server\\src\\main\\resources\\mapper");

        try {
            checkAndConvert(filepathList,map);
        }catch (Exception e){
        }
    }

    /**
     * 检查和转换
     * @param filepathList              需要检查和转换的路径列表
     * @param dbLinkNameOfUserName      dblink和用户名的键值对
     * @throws Exception
     */
    public static void checkAndConvert(List<String> filepathList,Map<String,String> dbLinkNameOfUserName) throws Exception{
        //检查是否存在dblink换行
        log.info("————————————————开始检查......");
        long startTime = System.currentTimeMillis();
        List<String> errorMessageList = check(filepathList);
        for(String errorMessage : errorMessageList){
            log.info(errorMessage);
        }
        assert errorMessageList.size() == 0 : "请先处理dblink换行问题！！";
        log.info("————————————————检查通过，共" + filepathList.size() + "个目录或文件，耗时：" + (System.currentTimeMillis() - startTime) + "ms");

        //转换
        log.info("————————————————开始转换......");
        startTime = System.currentTimeMillis();
        for(String filepath : filepathList){
            convert(filepath,dbLinkNameOfUserName);
        }
        log.info("————————————————转换成功，共" + filepathList.size() + "个目录或文件，耗时：" + (System.currentTimeMillis() - startTime) + "ms");
    }

    /**
     * 检查并搜集存在dblink换行的错误信息
     * @param filepathList      检查路径列表
     * @return
     * @throws Exception
     */
    private static List<String> check(List<String> filepathList) throws Exception{
        List<String> errorMessageList = new ArrayList<>();
        for(String filepath : filepathList){
            check(filepath,errorMessageList);
        }
        return errorMessageList;
    }

    /**
     * 检查并搜集存在dblink换行的错误信息
     * @param filepath              检查路径
     * @param errorMessageList      错误信息
     * @throws Exception
     */
    private static void check(String filepath,List<String> errorMessageList) throws Exception{
        File file = new File(filepath);
        if(file.isDirectory()){
            File[] childFileArr = file.listFiles();
            for(File childFile : childFileArr){
                check(childFile.getAbsolutePath(),errorMessageList);
            }
        }else{
            BufferedReader bufferedReader = null;
            try {
                bufferedReader = new BufferedReader(new FileReader(file));
                int lineCount = 1;
                String line = null;
                while( (line = bufferedReader.readLine()) != null ){
                    int dblinkIndex = -1;
                    if( (dblinkIndex = line.indexOf("@")) != -1 ){
                        String subPreLine = line.substring(0,dblinkIndex);
                        String subLastLine = line.substring(dblinkIndex + 1,line.length());
                        if("".equals(subPreLine.trim()) || "".equals(subLastLine.trim())){
                            String errorMessage = filepath + "第" + lineCount + "行，存在dblink换行！！！";
                            errorMessageList.add(errorMessage);
                        }
                    }
                    lineCount++;
                }
            }finally {
                if(null != bufferedReader){
                    bufferedReader.close();
                    bufferedReader = null;
                }
            }
        }
    }

    /**
     *  转换
     * @param filepath           转换路径
     * @param dbLinkNameOfUserName  dblink名称和user名称键值对
     */
    private static void convert(String filepath,Map<String,String> dbLinkNameOfUserName) throws Exception{
        long startTime = System.currentTimeMillis();
        File file = new File(filepath);
        if(file.isDirectory()){
            log.log(Level.INFO,"=============================开始转换目录：[" + file.getAbsolutePath() + "] ...");
            File[] childFileArr = file.listFiles();
            for(File childFile : childFileArr){
                convert(childFile.getAbsolutePath(),dbLinkNameOfUserName);
            }
        }else {
            //文件读取器
            BufferedReader bufIn = null;
            //文件写入器
            FileWriter out = null;
            try {
                bufIn = new BufferedReader(new FileReader(file));
                //临时流，关闭无效（无需关闭）
                CharArrayWriter tempStream = new CharArrayWriter();
                String line = null;

                //行计数器
                int lineCount = 1;
                while ( (line = bufIn.readLine()) != null) {
//                log.log(Level.INFO,"第" + lineCount + "行，正在处理...");
                    Iterator<String> it = dbLinkNameOfUserName.keySet().iterator();
                    while(it.hasNext()){
                        String dbLinkName = it.next();
                        String userName = dbLinkNameOfUserName.get(dbLinkName);
                        line = convertOfLine(line,dbLinkName,userName);
                    }
                    // 将该行写入内存
                    tempStream.write(line);
                    // 添加换行符
                    tempStream.append(System.getProperty("line.separator"));
                    lineCount++;
                }

                // 将内存中的流 写入 文件
                out = new FileWriter(file);
                tempStream.writeTo(out);

                log.log(Level.INFO,"[" + file.getAbsolutePath() + "]转换成功，耗时：" + (System.currentTimeMillis() - startTime) + "ms");
            }finally {
                // 关闭读取器
                if(null != bufIn){
                    bufIn.close();
                    bufIn = null;
                }
                // 关闭写入器
                if(null != out){
                    out.close();
                    out = null;
                }
            }
        }
    }

    /**
     * 转换（行）
     * @param lineStr       行串
     * @param dbLinkName    被替换的dblink名称
     * @param userName      用户名
     * @return
     */
    private static String convertOfLine(String lineStr,String dbLinkName,String userName){
        //@的下标
        int atIndex = -1;
        //查找@的开始下标
        int findAtStartIndex = 0;
        while( (atIndex = lineStr.indexOf("@",findAtStartIndex)) != -1 ){
            //dblink名称开始下标
            int dblinkNameFirstIndex = -1;
            //从当前@下标往后查找dblink名称的下标
            for(int i = atIndex + 1; i < lineStr.length(); i++){
                char c = lineStr.charAt(i);
                if(c != '\t' && c != ' '){
                    dblinkNameFirstIndex = i;
                    break;
                }
            }

            //从当前@后的dblink名称下标往后匹配指定的dblink名称的下标（是否匹配当前要转换的dblink）
            if(dblinkNameFirstIndex == lineStr.toUpperCase().indexOf(dbLinkName.toUpperCase(),dblinkNameFirstIndex)){
                //截取当前@与dblink名称及之间的字符串
                String atToDBLinkNameStr = lineStr.substring(atIndex,dblinkNameFirstIndex + dbLinkName.length());

                //从当前@下标往前查找表名最后一个字符的下标
                int tableNameLastIndex = -1;
                for(int i = atIndex - 1; i >= 0; i--){
                    char c = lineStr.charAt(i);
                    if(c != '\t' && c != ' '){
                        tableNameLastIndex = i;
                        break;
                    }
                }

                //截取表名最后一个字符及之前的字符串
                String toTableNameLastIndexStr = lineStr.substring(0,tableNameLastIndex + 1);
                //从子字符串中往前查找表名第一个字符的下标
                int tableNameFirstIndex = 0;
                for(int i = tableNameLastIndex; i >= 0; i--){
                    //当表名第一个字符的下标不是0，前一个字符必然是空格或tab(反之，表名前面没有tab或空格，表名第一个字符下标必然是0  ps：废话)
                    char c = toTableNameLastIndexStr.charAt(i);
                    if (c == '\t' || c == ' '){
                        tableNameFirstIndex = i + 1;
                        break;
                    }
                }

                StringBuffer sb = new StringBuffer(lineStr);
                //在表名前插入用户名
                sb.insert(tableNameFirstIndex,userName + ".");

                //将当前@与dblink名称及之间的字符串替换成空串
                String pattern = "(?i)" + atToDBLinkNameStr;
                lineStr = sb.toString().replaceFirst(pattern,"");
            }

            //查找下一个@的开始位置（即使存在dblink被替换，由于被替换的“atToDBLinkNameStr”后的第一个字符一定不是@
            // ，所以从当前@下标 + 1的位置不会导致漏查@
            // ，加上还有用户名的插入，甚至重复遍历当前@之前的字符）
            findAtStartIndex = atIndex + 1;
        }
        return lineStr;
    }
}
