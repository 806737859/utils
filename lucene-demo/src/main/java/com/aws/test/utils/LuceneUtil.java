package com.aws.test.utils;

import com.aws.test.utils.ik.IKAnalyzer5x;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 搜索工具类
 * 描述：建立文档索引，删除文档索引，修改文档索引，查询文档
 * 注：文档是索引的文档，对应的可以是文件或实体类
 * @author @AWS
 *
 */
public class LuceneUtil {
    /**
     * Text域类型-(索引、分词、存储)
     * 适用：摘要等
     */
    public static final FieldType TEXT_FIELD_TYPE_STORED = TextField.TYPE_STORED;

    /**
     * Text域类型-(索引、分词、不存储)
     * 适用：文件内容、大文本对象等
     */
    public static final FieldType TEXT_FIELD_TYPE_NOT_STORED = TextField.TYPE_NOT_STORED;

    /**
     * 字符串域类型-(索引、不分词、存储)
     * 适用：主键、文件路径、专有名词等
     */
    public static final FieldType STRING_FIELD_TYPE_STORED = StringField.TYPE_STORED;

    /**
     * 字符串域类型-(索引、不分词、不存储)
     * 适用：主键、文件路径、专有名词等
     */
    public static final FieldType STRING_FIELD_TYPE_NOT_STORED = StringField.TYPE_NOT_STORED;

    /**
     * 中文分词器-IK分词器
     */
    private static Analyzer analyzer = new IKAnalyzer5x(true);

    /**
     * 索引写入器容器-key:索引存放目录，value：目录的索引写入器
     */
    private static volatile Map<String,IndexWriter> indexWriterMap = new HashMap<String,IndexWriter>();

    /**
     * 索引读取器容器-key:索引存放目录，value：目录的索引读取器
     */
    private static volatile Map<String,DirectoryReader> indexReaderMap = new HashMap<String,DirectoryReader>();

    /**
     * 索引搜索器容器-key:索引存放目录，value：目录的索引搜索器
     */
    private static volatile Map<String,IndexSearcher> IndexSearcherMap = new HashMap<String,IndexSearcher>();

    /**
     * 索引写入器锁
     */
    private static final Object writerLock = new Object();

    /**
     * 索引读取器锁
     */
    private static final Object readerLock = new Object();

    /**
     * 索引搜索器锁
     */
    private static final Object searcherLock = new Object();

    /**
     * 构造器私有
     */
    private LuceneUtil(){

    }

    /**
     * 域模型
     * 描述：建立文档索引的域模型-多个不同域名的域模型为一个文档
     * @author @AWS
     *
     */
    public static class FieldModel{
        /**
         * 域名
         */
        private String fieldName;
        /**
         * 域值
         */
        private String fieldValue;
        /**
         * 域类型
         */
        private FieldType fieldType;
        /**
         * 主键标志
         */
        private boolean idFlag;
        /**
         * 高亮标志
         */
        private boolean highlightFlag;

        public FieldModel(String fieldName,String fieldValue,FieldType fieldType,boolean idFlag,boolean highlightFlag){
            this.fieldName = fieldName;
            this.fieldValue = fieldValue;
            this.fieldType = fieldType;
            this.idFlag = idFlag;
            this.highlightFlag = highlightFlag;
        }

        public String getFieldName() {
            return fieldName;
        }
        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }
        public String getFieldValue() {
            return fieldValue;
        }
        public void setFieldValue(String fieldValue) {
            this.fieldValue = fieldValue;
        }
        public FieldType getFieldType() {
            return fieldType;
        }
        public void setFieldType(FieldType fieldType) {
            this.fieldType = fieldType;
        }
        public boolean getIdFlag() {
            return idFlag;
        }
        public void setIdFlag(boolean idFlag) {
            this.idFlag = idFlag;
        }
        public boolean getHighlightFlag() {
            return highlightFlag;
        }
        public void setHighlightFlag(boolean highlightFlag) {
            this.highlightFlag = highlightFlag;
        }
    }

    /**
     * 新增索引
     * @param indexPath		索引存储位置
     * @param docs			多个文档模型（[对象][字段]）-文档需要建立主键模型
     * @throws Exception
     */
    public static void addIndex(String indexPath, FieldModel[]... docs) throws Exception{
        IndexWriter iw = null;
        if (indexPath == null || docs == null) {
            throw new IllegalArgumentException("传入参数为空！");
        }

        try{
            //加入索引文档
            iw = getIndexWriter(indexPath);
            for(FieldModel[] doc : docs){
                Document document = new Document();
                for(FieldModel col : doc){
                    document.add(new Field(col.getFieldName(),col.getFieldValue(),col.getFieldType()));
                }
                iw.addDocument(document);
            }

            //提交事务
            iw.commit();
        }catch(Exception e){
            e.printStackTrace();
            //事务回滚
            if(iw != null){
                iw.rollback();
            }
            throw new RuntimeException("新增索引失败");
        }
    }

    /**
     * 删除索引-依据文档的主键
     * @param indexPath		索引存储位置
     * @param idColArr		主键字段模型数组（[主键]）-必须与建立文档索引时的主键一致，否则删除失败或异常
     * @throws Exception
     */
    public static void deleteIndex(String indexPath, FieldModel[] idColArr) throws Exception{
        IndexWriter iw = null;
        if(indexPath == null || idColArr == null){
            throw new IllegalArgumentException("传入参数为空！");
        }
        try{
            iw = getIndexWriter(indexPath);
            Term[] termArr = new Term[idColArr.length];
            for(int i = 0; i < idColArr.length; i++){
                FieldModel idCol = idColArr[i];
                if(idCol.getIdFlag()){
                    termArr[i] = new Term(idColArr[i].getFieldName(),idColArr[i].getFieldValue());
                }
            }
            //删除文档索引
            iw.deleteDocuments(termArr);
            //提交事务
            iw.commit();
        }catch(Exception e){
            e.printStackTrace();
            //事务回滚
            if(iw != null){
                iw.rollback();
            }
            throw new RuntimeException();
        }
    }

    /**
     * 修改索引-依据文档id更新索引(删除旧索引，新增索引)
     * @param indexPath		索引存储位置
     * @param docs			需要更新的文档模型数组（每个域必须包含：域名、域值、域类型）
     * @throws Exception
     */
    public static void updateIndex(String indexPath, FieldModel[]... docs) throws Exception{
        IndexWriter iw = null;
        if(indexPath == null || docs == null){
            throw new IllegalArgumentException("传入参数为空！");
        }
        try{
            iw = getIndexWriter(indexPath);
            for(FieldModel[] doc : docs){
                //当前文档id
                Term idTerm = null;
                Document document = new Document();
                for(FieldModel col : doc){
                    document.add(new Field(col.getFieldName(),col.getFieldValue(),col.getFieldType()));
                    if(col.getIdFlag()){
                        idTerm = new Term(col.getFieldName(),col.getFieldValue());
                    }
                }
                //查找满足term的文档并替换
                iw.updateDocument(idTerm, document);
            }
            //提交事务
            iw.commit();
        }catch(Exception e){
            e.printStackTrace();
            //事务回滚
            if(iw != null){
                iw.rollback();
            }
            throw new RuntimeException("修改索引异常！");
        }
    }

    /**
     * 搜索文档
     * @param indexPath			索引存储位置
     * @param queryStr			搜索字符串
     * @param searchColNames	搜索范围[字段名-建立索引时的域名]
     * @param doc				提取文档的模型 - 注：Model必须设置字段名
     * @param size				搜索数量
     * @return					返回文档模型结果集
     * @throws Exception
     */
    public static List<FieldModel[]> search(String indexPath, String queryStr, String[] searchColNames,
                                            FieldModel[] doc, int size) throws Exception {
        List<FieldModel[]> resDocList = null;
        if(indexPath == null || queryStr == null || searchColNames == null || doc == null){
            throw new IllegalArgumentException("传入参数为空！");
        }
        if (size > 0) {
            IndexSearcher is = getIndexSearcher(indexPath);
            MultiFieldQueryParser mqp = new MultiFieldQueryParser(searchColNames, analyzer);
            Query query = mqp.parse(queryStr);

            //高亮分析器
            Highlighter highlighter = null;

            //搜索的结果集
            TopDocs topDocs = is.search(query, size);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;

            //提取结果集
            resDocList = new ArrayList<FieldModel[]>();
            for(ScoreDoc sd : scoreDocs){
                //返回的文档模型
                FieldModel[] resDoc = new FieldModel[doc.length];
                Document document = is.doc(sd.doc);
                for(int i = 0; i < doc.length; i++){
                    //需要提取的文档模型的字段
                    FieldModel col = doc[i];

                    String colName = col.getFieldName();
                    String colValue = document.get(colName);

                    //高亮
                    if(colValue != null && col.getHighlightFlag()){
                        highlighter = highlighter != null ? highlighter : getHighlighter(query,colName);
                        TokenStream ts = analyzer.tokenStream(colName, colValue);
                        String highStr = highlighter.getBestFragment(ts, colValue);
                        if(highStr != null){
                            colValue = highStr;
                        }
                    }

                    //返回的文档的字段模型
                    FieldModel resCol = new FieldModel(colName, colValue,
                            col.getFieldType(), col.getIdFlag(),
                            col.getHighlightFlag());

                    //加入文档字段
                    resDoc[i] = resCol;
                }
                resDocList.add(resDoc);
            }
        }
        return resDocList;
    }

    /**
     * 分页搜索文档-检索内容
     * @param indexPath			索引存储位置
     * @param queryStr			搜索字符串
     * @param searchColNames	搜索范围[字段名-建立索引时的域名]
     * @param doc				提取文档的模型 - 注：Model必须设置字段名
     * @param pageIndex			当前页码
     * @param pageSize			每页数量
     * @return					返回文档模型结果集
     * @throws Exception
     */
    public static List<FieldModel[]> searchPage(String indexPath,
                                                String queryStr, String[] searchColNames, FieldModel[] doc,
                                                int pageIndex, int pageSize) throws Exception {
        List<FieldModel[]> resDocList = null;
        if(indexPath == null || queryStr == null || searchColNames == null || doc == null){
            throw new IllegalArgumentException("传入参数为空！");
        }
        if (pageIndex > 0 && pageSize > 0) {
            if(pageIndex == 1){
                resDocList = search(indexPath,queryStr,searchColNames,doc,pageSize);
            }else{
                IndexSearcher is = getIndexSearcher(indexPath);
                MultiFieldQueryParser mqp = new MultiFieldQueryParser(searchColNames, analyzer);
                Query query = mqp.parse(queryStr);

                //高亮分析器
                Highlighter highlighter = null;

                //上一页最后数量
                int num = (pageIndex - 1) * pageSize;
                //获取上一页的最后一个结果
                ScoreDoc lastScoreDoc = is.search(query, num).scoreDocs[num - 1];

                //提取结果集
                TopDocs topDocs = is.searchAfter(lastScoreDoc, query, pageSize);
                ScoreDoc[] scoreDocs = topDocs.scoreDocs;

                //提取结果集
                resDocList = new ArrayList<FieldModel[]>();
                for (ScoreDoc sd : scoreDocs) {
                    //返回的文档模型
                    FieldModel[] resDoc = new FieldModel[doc.length];
                    Document document = is.doc(sd.doc);
                    for(int i = 0; i < doc.length; i++){
                        //需要提取的文档模型的字段
                        FieldModel col = doc[i];

                        String colName = col.getFieldName();
                        String colValue = document.get(colName);

                        //高亮
                        if(colValue != null && col.getHighlightFlag()){
                            highlighter = highlighter != null ? highlighter : getHighlighter(query,colName);
                            TokenStream ts = analyzer.tokenStream(colName, colValue);
                            colValue = highlighter.getBestFragment(ts, colValue);
                        }

                        //返回的模型
                        FieldModel resCol = new FieldModel(colName, colValue,
                                col.getFieldType(), col.getIdFlag(),
                                col.getHighlightFlag());

                        //加入文档字段
                        resDoc[i] = resCol;
                    }
                    resDocList.add(resDoc);
                }
            }
        }
        return resDocList;
    }

    /**
     * 统计匹配查询的记录条数
     * @param indexPath			索引存储位置
     * @param queryStr			搜索字符串
     * @param searchColNames	搜索范围[字段名-建立索引时的域名]
     * @return
     * @throws Exception
     */
    public static int count(String indexPath,String queryStr,String[] searchColNames) throws Exception {
        if(indexPath == null || queryStr == null || searchColNames == null){
            throw new IllegalArgumentException("传入参数为空！");
        }
        QueryParser mqp = new MultiFieldQueryParser(searchColNames, analyzer);
        Query query = mqp.parse(queryStr);
        IndexSearcher is = getIndexSearcher(indexPath);
        return is.count(query);
    }

    /**
     * 字符串高亮处理
     * @param query			查询关键字
     * @param colValue		需要高亮的字段值
     * @return				高亮后的字符串
     * @throws Exception
     */
    public static String highlightHandle(String query,String colValue) throws Exception{
        Query q = new QueryParser(null, analyzer).parse(query);
        TokenStream ts = analyzer.tokenStream(null, colValue);
        return getHighlighter(q,null).getBestFragment(ts, colValue);
    }

    /**
     * 获取索引写入器
     * @param indexPath		索引存储路径
     * @return
     * @throws Exception
     */
    private static IndexWriter getIndexWriter(String indexPath) throws Exception {
        IndexWriter iw = indexWriterMap.get(indexPath);
        try{
            if(iw == null || !iw.isOpen()){
                synchronized (writerLock) {
                    iw = indexWriterMap.get(indexPath);
                    if(iw == null || !iw.isOpen()){
                        Directory directory = openFSDirectory(indexPath);
                        IndexWriterConfig iwc = getIndexWriterConfig();
                        iw = new IndexWriter(directory,iwc);
                        indexWriterMap.put(indexPath, iw);
                    }
                }
            }
        }catch(Exception e){
            try{
                if(iw != null){
                    iw.rollback();
                    iw.close();
                    iw = null;
                }
            }catch(Exception e2){
                e2.printStackTrace();
            }
            indexWriterMap.remove(indexPath);
            e.printStackTrace();
            throw new RuntimeException("获取索引时异常!");
        }
        return iw;
    }

    /**
     * 获取索引读取器
     * @param indexPath		索引存储路径
     * @return
     * @throws Exception
     */
    private static IndexReader getIndexReader(String indexPath) throws Exception {
        //最新时间点的读取器
        DirectoryReader newReader = null;
        //旧时间点的读取器
        DirectoryReader oldReader = indexReaderMap.get(indexPath);

        try{
            if(oldReader == null || !oldReader.isCurrent()){
                synchronized (readerLock) {
                    oldReader = indexReaderMap.get(indexPath);
                    if(oldReader == null){
                        newReader = DirectoryReader.open(getIndexWriter(indexPath), true);
                        indexReaderMap.put(indexPath, newReader);
                    }else{
                        if(!oldReader.isCurrent()){
                            newReader = DirectoryReader.openIfChanged(oldReader);
                            indexReaderMap.put(indexPath, newReader);
                            oldReader.close();
                        }else{
                            newReader = oldReader;
                        }
                    }
                }
            }else{
                newReader = oldReader;
            }
        }catch(Exception e){
            try{
                if(oldReader != null){
                    oldReader.close();
                    oldReader = null;
                }
                if(newReader != null){
                    newReader.close();
                    newReader = null;
                }
            }catch(Exception e2){
                e2.printStackTrace();
            }
            indexReaderMap.remove(indexPath);
            e.printStackTrace();
            throw new RuntimeException("获取索引读取器时异常!");
        }
        return newReader;
    }

    /**
     * 获取索引搜索器
     * @param indexPath		索引存储路径
     * @return
     * @throws Exception
     */
    private static IndexSearcher getIndexSearcher(String indexPath) throws Exception{
        IndexSearcher is = IndexSearcherMap.get(indexPath);
        try{
            if(is == null || !((DirectoryReader)is.getIndexReader()).isCurrent()){
                synchronized(searcherLock){
                    is = IndexSearcherMap.get(indexPath);
                    if(is == null || !((DirectoryReader)is.getIndexReader()).isCurrent()){
                        is = new IndexSearcher(getIndexReader(indexPath));
                        IndexSearcherMap.put(indexPath, is);
                    }
                }
            }
        }catch(Exception e){
            is = null;
            IndexSearcherMap.remove(indexPath);
            throw new RuntimeException("获取索引搜索器异常!");
        }
        return is;
    }

    /**
     * 打开索引目录
     * @param indexPath		索引存储路径
     * @return
     * @throws Exception
     */
    private static Directory openFSDirectory(String indexPath) throws Exception{
        Directory dir = null;
        try{
            dir = FSDirectory.open(Paths.get(indexPath));
        }catch(Exception e){
            if(dir != null){
                dir.close();
                dir = null;
            }
            e.printStackTrace();
            throw new RuntimeException("打开索引目录时异常!");
        }
        return dir;
    }

    /**
     * 获取索引写入器配置
     * @return
     */
    private static IndexWriterConfig getIndexWriterConfig(){
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        //进行扩展配置
        return iwc;
    }

    /**
     * 获取高亮分析器
     * @param query		查询对象
     * @param colName	高亮字段名-建立索引时的域名
     * @return			高亮分析器
     */
    private static Highlighter getHighlighter(Query query,String colName){
        //定制高亮标签
        SimpleHTMLFormatter shf = new SimpleHTMLFormatter("<font color='red'>", "</font>");
        //评分器-用于评分文本片段(基于查询词的数量)
        QueryScorer qs = new QueryScorer(query,colName);
        //高亮分析器
        return new Highlighter(shf,qs);
    }

    public static void main(String[] args){
        try{
            //索引存储位置
//			String indexPath = "F:/luceneIndex";

            //测试用例
            //文档1 包含两个字段：用户名、用户描述
//			FieldModel[] doc1 = new FieldModel[2];
//			doc1[0] = new FieldModel("username", "用户1", LuceneUtil.STRING_FIELD_TYPE_STORED, true, false);
//			doc1[1] = new FieldModel("desc", "这个是用户1", LuceneUtil.TEXT_FIELD_TYPE_STORED, false, true);
            //文档2 包含两个字段：用户名、用户描述
//			FieldModel[] doc2 = new FieldModel[2];
//			doc2[0] = new FieldModel("username", "用户2", LuceneUtil.STRING_FIELD_TYPE_STORED, true, false);
//			doc2[1] = new FieldModel("desc", "这个是用户2", LuceneUtil.TEXT_FIELD_TYPE_STORED, false, true);

            //测试新建索引
            //需要索引的文档
//			FieldModel[][] docArr = {doc1,doc2};
//			addIndex(indexPath,docArr);

            //测试删除索引
            //需要删除的文档的主键域
//			FieldModel[] idFieldArr = {doc1[0],doc2[0]};
//			deleteIndex(indexPath,idFieldArr);

            //测试修改索引
            //需要更新的文档
//			FieldModel[][] newDocArr = {doc1,doc2};
//			updateIndex(indexPath,newDocArr);

            //测试搜索
            //搜索的范围
//			String[] searchRange = {"desc"};
            //搜索的文档模型
//			FieldModel fm1 = new FieldModel("username", null, null, true, false);
//			FieldModel fm2 = new FieldModel("desc", null, null, false, true);
//			FieldModel[] docModel = {fm1,fm2};
//			List<FieldModel[]> resultList1 = search(indexPath, "用户", searchRange, docModel, 10);
            //分页 搜索第1页 每页10条数据
//			List<FieldModel[]> resultList2 = searchPage(indexPath, "用户", searchRange, docModel, 1, 10);
            //遍历搜索结果
//			for(FieldModel[] doc : resultList1){
//				for(FieldModel column : doc){
//					System.out.println("字段名：" + column.getFieldName() + "----字段值：" + column.getFieldValue());
//				}
//			}
        }catch(Exception e){
            e.printStackTrace();
//			System.out.println("报错");
        }
    }
}
