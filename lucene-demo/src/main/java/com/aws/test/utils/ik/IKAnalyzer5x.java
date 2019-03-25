package com.aws.test.utils.ik;

import org.apache.lucene.analysis.Analyzer;

/**
 * 支持lucene5.x版本的IKAnalyzer
 * 来源：http://blog.csdn.net/isea533/article/details/50186963
 * @author @AWS
 *
 */
public final class IKAnalyzer5x extends Analyzer {
    private boolean useSmart;

    public boolean useSmart() {
        return this.useSmart;
    }

    public void setUseSmart(boolean useSmart) {
        this.useSmart = useSmart;
    }

    public IKAnalyzer5x() {
        this(false);
    }

    public IKAnalyzer5x(boolean useSmart) {
        this.useSmart = useSmart;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        IKTokenizer5x _IKTokenizer = new IKTokenizer5x(this.useSmart);
        return new TokenStreamComponents(_IKTokenizer);
    }
}
