package org.fields.aiplatformmetadata;

import org.fields.aiplatformmetadata.metadata.Utils;
import org.junit.Assert;
import org.junit.Test;

public class ScriptTest {
    @Test
    public void test(){
        String[] args = new String[3];
        args[0] = "000001.SZ";
        args[1] = "20190603";
        args[2] = "high";
        String ret = Utils.callScript(args);
        System.out.println(ret + ", " +  ret.length());
        Assert.assertEquals(ret, "12.33");
    }
}
