/*
 * This file is generated by jOOQ.
 */
package edu.uci.ics.texera.web.model.jooq.generated.tables.pojos;


import edu.uci.ics.texera.web.model.jooq.generated.tables.interfaces.IKeywordDictionary;

import org.jooq.types.UInteger;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class KeywordDictionary implements IKeywordDictionary {

    private static final long serialVersionUID = 677434385;

    private UInteger uid;
    private UInteger kid;
    private String   name;
    private byte[]   content;
    private String   description;

    public KeywordDictionary() {}

    public KeywordDictionary(IKeywordDictionary value) {
        this.uid = value.getUid();
        this.kid = value.getKid();
        this.name = value.getName();
        this.content = value.getContent();
        this.description = value.getDescription();
    }

    public KeywordDictionary(
        UInteger uid,
        UInteger kid,
        String   name,
        byte[]   content,
        String   description
    ) {
        this.uid = uid;
        this.kid = kid;
        this.name = name;
        this.content = content;
        this.description = description;
    }

    @Override
    public UInteger getUid() {
        return this.uid;
    }

    @Override
    public void setUid(UInteger uid) {
        this.uid = uid;
    }

    @Override
    public UInteger getKid() {
        return this.kid;
    }

    @Override
    public void setKid(UInteger kid) {
        this.kid = kid;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public byte[] getContent() {
        return this.content;
    }

    @Override
    public void setContent(byte... content) {
        this.content = content;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("KeywordDictionary (");

        sb.append(uid);
        sb.append(", ").append(kid);
        sb.append(", ").append(name);
        sb.append(", ").append("[binary...]");
        sb.append(", ").append(description);

        sb.append(")");
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // FROM and INTO
    // -------------------------------------------------------------------------

    @Override
    public void from(IKeywordDictionary from) {
        setUid(from.getUid());
        setKid(from.getKid());
        setName(from.getName());
        setContent(from.getContent());
        setDescription(from.getDescription());
    }

    @Override
    public <E extends IKeywordDictionary> E into(E into) {
        into.from(this);
        return into;
    }
}
