package com.mq.proxy.core.protocol;

public enum LanguageCode {
    JAVA((byte) 0),
    CPP((byte) 1),
    DOTNET((byte) 2),
    PYTHON((byte) 3),
    DELPHI((byte) 4),
    ERLANG((byte) 5),
    RUBY((byte) 6),
    OTHER((byte) 7),
    GO((byte) 8),
    PHP((byte) 9),
    OM((byte) 10),
    RUST((byte) 11);

    private byte code;

    LanguageCode(byte code) {
        this.code = code;
    }

    public static LanguageCode forCode(byte code) {
        for (LanguageCode languageCode : LanguageCode.values()) {
            if (languageCode.getCode() == code) {
                return languageCode;
            }
        }
        return null;
    }

    public byte getCode() {
        return code;
    }
}
