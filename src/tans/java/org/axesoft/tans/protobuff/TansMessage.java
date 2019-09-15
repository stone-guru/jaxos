// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/tans/proto/tans-message.proto

package org.axesoft.tans.protobuff;

public final class TansMessage {
  private TansMessage() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface ProtoTansNumberOrBuilder extends
      // @@protoc_insertion_point(interface_extends:org.axesoft.tans.protobuff.ProtoTansNumber)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional string name = 1;</code>
     */
    java.lang.String getName();
    /**
     * <code>optional string name = 1;</code>
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>optional int64 value = 2;</code>
     */
    long getValue();

    /**
     * <code>optional int64 version = 3;</code>
     */
    long getVersion();

    /**
     * <code>optional int64 timestamp = 4;</code>
     */
    long getTimestamp();

    /**
     * <code>optional int64 version0 = 5;</code>
     */
    long getVersion0();

    /**
     * <code>optional int64 value0 = 6;</code>
     */
    long getValue0();
  }
  /**
   * Protobuf type {@code org.axesoft.tans.protobuff.ProtoTansNumber}
   */
  public  static final class ProtoTansNumber extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:org.axesoft.tans.protobuff.ProtoTansNumber)
      ProtoTansNumberOrBuilder {
    // Use ProtoTansNumber.newBuilder() to construct.
    private ProtoTansNumber(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private ProtoTansNumber() {
      name_ = "";
      value_ = 0L;
      version_ = 0L;
      timestamp_ = 0L;
      version0_ = 0L;
      value0_ = 0L;
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
    }
    private ProtoTansNumber(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      int mutable_bitField0_ = 0;
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!input.skipField(tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              java.lang.String s = input.readStringRequireUtf8();

              name_ = s;
              break;
            }
            case 16: {

              value_ = input.readInt64();
              break;
            }
            case 24: {

              version_ = input.readInt64();
              break;
            }
            case 32: {

              timestamp_ = input.readInt64();
              break;
            }
            case 40: {

              version0_ = input.readInt64();
              break;
            }
            case 48: {

              value0_ = input.readInt64();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.axesoft.tans.protobuff.TansMessage.internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.axesoft.tans.protobuff.TansMessage.internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber.class, org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber.Builder.class);
    }

    public static final int NAME_FIELD_NUMBER = 1;
    private volatile java.lang.Object name_;
    /**
     * <code>optional string name = 1;</code>
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        name_ = s;
        return s;
      }
    }
    /**
     * <code>optional string name = 1;</code>
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int VALUE_FIELD_NUMBER = 2;
    private long value_;
    /**
     * <code>optional int64 value = 2;</code>
     */
    public long getValue() {
      return value_;
    }

    public static final int VERSION_FIELD_NUMBER = 3;
    private long version_;
    /**
     * <code>optional int64 version = 3;</code>
     */
    public long getVersion() {
      return version_;
    }

    public static final int TIMESTAMP_FIELD_NUMBER = 4;
    private long timestamp_;
    /**
     * <code>optional int64 timestamp = 4;</code>
     */
    public long getTimestamp() {
      return timestamp_;
    }

    public static final int VERSION0_FIELD_NUMBER = 5;
    private long version0_;
    /**
     * <code>optional int64 version0 = 5;</code>
     */
    public long getVersion0() {
      return version0_;
    }

    public static final int VALUE0_FIELD_NUMBER = 6;
    private long value0_;
    /**
     * <code>optional int64 value0 = 6;</code>
     */
    public long getValue0() {
      return value0_;
    }

    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (!getNameBytes().isEmpty()) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, name_);
      }
      if (value_ != 0L) {
        output.writeInt64(2, value_);
      }
      if (version_ != 0L) {
        output.writeInt64(3, version_);
      }
      if (timestamp_ != 0L) {
        output.writeInt64(4, timestamp_);
      }
      if (version0_ != 0L) {
        output.writeInt64(5, version0_);
      }
      if (value0_ != 0L) {
        output.writeInt64(6, value0_);
      }
    }

    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!getNameBytes().isEmpty()) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, name_);
      }
      if (value_ != 0L) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, value_);
      }
      if (version_ != 0L) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(3, version_);
      }
      if (timestamp_ != 0L) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(4, timestamp_);
      }
      if (version0_ != 0L) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(5, version0_);
      }
      if (value0_ != 0L) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(6, value0_);
      }
      memoizedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber)) {
        return super.equals(obj);
      }
      org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber other = (org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber) obj;

      boolean result = true;
      result = result && getName()
          .equals(other.getName());
      result = result && (getValue()
          == other.getValue());
      result = result && (getVersion()
          == other.getVersion());
      result = result && (getTimestamp()
          == other.getTimestamp());
      result = result && (getVersion0()
          == other.getVersion0());
      result = result && (getValue0()
          == other.getValue0());
      return result;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      hash = (37 * hash) + NAME_FIELD_NUMBER;
      hash = (53 * hash) + getName().hashCode();
      hash = (37 * hash) + VALUE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getValue());
      hash = (37 * hash) + VERSION_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getVersion());
      hash = (37 * hash) + TIMESTAMP_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getTimestamp());
      hash = (37 * hash) + VERSION0_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getVersion0());
      hash = (37 * hash) + VALUE0_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
          getValue0());
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code org.axesoft.tans.protobuff.ProtoTansNumber}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:org.axesoft.tans.protobuff.ProtoTansNumber)
        org.axesoft.tans.protobuff.TansMessage.ProtoTansNumberOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.axesoft.tans.protobuff.TansMessage.internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_descriptor;
      }

      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.axesoft.tans.protobuff.TansMessage.internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber.class, org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber.Builder.class);
      }

      // Construct using org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      public Builder clear() {
        super.clear();
        name_ = "";

        value_ = 0L;

        version_ = 0L;

        timestamp_ = 0L;

        version0_ = 0L;

        value0_ = 0L;

        return this;
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.axesoft.tans.protobuff.TansMessage.internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_descriptor;
      }

      public org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber getDefaultInstanceForType() {
        return org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber.getDefaultInstance();
      }

      public org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber build() {
        org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber buildPartial() {
        org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber result = new org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber(this);
        result.name_ = name_;
        result.value_ = value_;
        result.version_ = version_;
        result.timestamp_ = timestamp_;
        result.version0_ = version0_;
        result.value0_ = value0_;
        onBuilt();
        return result;
      }

      public Builder clone() {
        return (Builder) super.clone();
      }
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.setField(field, value);
      }
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return (Builder) super.clearField(field);
      }
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return (Builder) super.clearOneof(oneof);
      }
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, Object value) {
        return (Builder) super.setRepeatedField(field, index, value);
      }
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.addRepeatedField(field, value);
      }
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber) {
          return mergeFrom((org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber other) {
        if (other == org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber.getDefaultInstance()) return this;
        if (!other.getName().isEmpty()) {
          name_ = other.name_;
          onChanged();
        }
        if (other.getValue() != 0L) {
          setValue(other.getValue());
        }
        if (other.getVersion() != 0L) {
          setVersion(other.getVersion());
        }
        if (other.getTimestamp() != 0L) {
          setTimestamp(other.getTimestamp());
        }
        if (other.getVersion0() != 0L) {
          setVersion0(other.getVersion0());
        }
        if (other.getValue0() != 0L) {
          setValue0(other.getValue0());
        }
        onChanged();
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private java.lang.Object name_ = "";
      /**
       * <code>optional string name = 1;</code>
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          name_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string name = 1;</code>
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string name = 1;</code>
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string name = 1;</code>
       */
      public Builder clearName() {
        
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>optional string name = 1;</code>
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        name_ = value;
        onChanged();
        return this;
      }

      private long value_ ;
      /**
       * <code>optional int64 value = 2;</code>
       */
      public long getValue() {
        return value_;
      }
      /**
       * <code>optional int64 value = 2;</code>
       */
      public Builder setValue(long value) {
        
        value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 value = 2;</code>
       */
      public Builder clearValue() {
        
        value_ = 0L;
        onChanged();
        return this;
      }

      private long version_ ;
      /**
       * <code>optional int64 version = 3;</code>
       */
      public long getVersion() {
        return version_;
      }
      /**
       * <code>optional int64 version = 3;</code>
       */
      public Builder setVersion(long value) {
        
        version_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 version = 3;</code>
       */
      public Builder clearVersion() {
        
        version_ = 0L;
        onChanged();
        return this;
      }

      private long timestamp_ ;
      /**
       * <code>optional int64 timestamp = 4;</code>
       */
      public long getTimestamp() {
        return timestamp_;
      }
      /**
       * <code>optional int64 timestamp = 4;</code>
       */
      public Builder setTimestamp(long value) {
        
        timestamp_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 timestamp = 4;</code>
       */
      public Builder clearTimestamp() {
        
        timestamp_ = 0L;
        onChanged();
        return this;
      }

      private long version0_ ;
      /**
       * <code>optional int64 version0 = 5;</code>
       */
      public long getVersion0() {
        return version0_;
      }
      /**
       * <code>optional int64 version0 = 5;</code>
       */
      public Builder setVersion0(long value) {
        
        version0_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 version0 = 5;</code>
       */
      public Builder clearVersion0() {
        
        version0_ = 0L;
        onChanged();
        return this;
      }

      private long value0_ ;
      /**
       * <code>optional int64 value0 = 6;</code>
       */
      public long getValue0() {
        return value0_;
      }
      /**
       * <code>optional int64 value0 = 6;</code>
       */
      public Builder setValue0(long value) {
        
        value0_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 value0 = 6;</code>
       */
      public Builder clearValue0() {
        
        value0_ = 0L;
        onChanged();
        return this;
      }
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return this;
      }

      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return this;
      }


      // @@protoc_insertion_point(builder_scope:org.axesoft.tans.protobuff.ProtoTansNumber)
    }

    // @@protoc_insertion_point(class_scope:org.axesoft.tans.protobuff.ProtoTansNumber)
    private static final org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber();
    }

    public static org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<ProtoTansNumber>
        PARSER = new com.google.protobuf.AbstractParser<ProtoTansNumber>() {
      public ProtoTansNumber parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
          return new ProtoTansNumber(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<ProtoTansNumber> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<ProtoTansNumber> getParserForType() {
      return PARSER;
    }

    public org.axesoft.tans.protobuff.TansMessage.ProtoTansNumber getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n!src/tans/proto/tans-message.proto\022\032org" +
      ".axesoft.tans.protobuff\"t\n\017ProtoTansNumb" +
      "er\022\014\n\004name\030\001 \001(\t\022\r\n\005value\030\002 \001(\003\022\017\n\007versi" +
      "on\030\003 \001(\003\022\021\n\ttimestamp\030\004 \001(\003\022\020\n\010version0\030" +
      "\005 \001(\003\022\016\n\006value0\030\006 \001(\003B\rB\013TansMessageb\006pr" +
      "oto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_axesoft_tans_protobuff_ProtoTansNumber_descriptor,
        new java.lang.String[] { "Name", "Value", "Version", "Timestamp", "Version0", "Value0", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
