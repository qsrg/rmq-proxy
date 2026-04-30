package com.mq.proxy.core.storage.model;

public class OffsetResult {
    private boolean success;
    private long offset;
    private int responseCode;
    private String remark;

    public static OffsetResult success(long offset) {
        OffsetResult result = new OffsetResult();
        result.setSuccess(true);
        result.setOffset(offset);
        result.setResponseCode(0);
        return result;
    }

    public static OffsetResult fail(int responseCode, String remark) {
        OffsetResult result = new OffsetResult();
        result.setSuccess(false);
        result.setResponseCode(responseCode);
        result.setRemark(remark);
        return result;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}
