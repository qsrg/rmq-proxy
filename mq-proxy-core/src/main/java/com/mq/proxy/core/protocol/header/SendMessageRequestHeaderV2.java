package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class SendMessageRequestHeaderV2 implements CommandCustomHeader {
    private String a;
    private String b;
    private String c;
    private Integer d;
    private Integer e;
    private Integer f;
    private Long g;
    private Integer h;
    private String i;
    private Integer j;
    private boolean k = false;
    private Integer l;
    private boolean m = false;
    private String n;

    public static SendMessageRequestHeader createSendMessageRequestHeaderV1(SendMessageRequestHeaderV2 v2) {
        SendMessageRequestHeader v1 = new SendMessageRequestHeader();
        v1.setProducerGroup(v2.a);
        v1.setTopic(v2.b);
        v1.setDefaultTopic(v2.c);
        v1.setDefaultTopicQueueNums(v2.d);
        v1.setQueueId(v2.e);
        v1.setSysFlag(v2.f);
        v1.setBornTimestamp(v2.g);
        v1.setFlag(v2.h);
        v1.setProperties(v2.i);
        v1.setReconsumeTimes(v2.j);
        v1.setUnitMode(v2.k);
        v1.setMaxReconsumeTimes(v2.l);
        v1.setBatch(v2.m);
        v1.setBname(v2.n);
        return v1;
    }

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (a != null) {
            map.put("a", a);
        }
        if (b != null) {
            map.put("b", b);
        }
        if (c != null) {
            map.put("c", c);
        }
        if (d != null) {
            map.put("d", String.valueOf(d));
        }
        if (e != null) {
            map.put("e", String.valueOf(e));
        }
        if (f != null) {
            map.put("f", String.valueOf(f));
        }
        if (g != null) {
            map.put("g", String.valueOf(g));
        }
        if (h != null) {
            map.put("h", String.valueOf(h));
        }
        if (i != null) {
            map.put("i", i);
        }
        if (j != null) {
            map.put("j", String.valueOf(j));
        }
        map.put("k", String.valueOf(k));
        if (l != null) {
            map.put("l", String.valueOf(l));
        }
        map.put("m", String.valueOf(m));
        if (n != null) {
            map.put("n", n);
        }
        return map;
    }

    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    public String getC() {
        return c;
    }

    public void setC(String c) {
        this.c = c;
    }

    public Integer getD() {
        return d;
    }

    public void setD(Integer d) {
        this.d = d;
    }

    public Integer getE() {
        return e;
    }

    public void setE(Integer e) {
        this.e = e;
    }

    public Integer getF() {
        return f;
    }

    public void setF(Integer f) {
        this.f = f;
    }

    public Long getG() {
        return g;
    }

    public void setG(Long g) {
        this.g = g;
    }

    public Integer getH() {
        return h;
    }

    public void setH(Integer h) {
        this.h = h;
    }

    public String getI() {
        return i;
    }

    public void setI(String i) {
        this.i = i;
    }

    public Integer getJ() {
        return j;
    }

    public void setJ(Integer j) {
        this.j = j;
    }

    public boolean isK() {
        return k;
    }

    public void setK(boolean k) {
        this.k = k;
    }

    public Integer getL() {
        return l;
    }

    public void setL(Integer l) {
        this.l = l;
    }

    public boolean isM() {
        return m;
    }

    public void setM(boolean m) {
        this.m = m;
    }

    public String getN() {
        return n;
    }

    public void setN(String n) {
        this.n = n;
    }
}
