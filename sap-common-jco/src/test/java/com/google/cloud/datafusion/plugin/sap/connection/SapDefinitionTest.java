/*
 * Copyright © 2021 Cask Data, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.datafusion.plugin.sap.connection;

import com.google.cloud.datafusion.plugin.util.PayloadHelper;
import com.sap.conn.jco.ext.DestinationDataProvider;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * @author sankalpbapat
 *
 */
public class SapDefinitionTest {

  public static final String EMPTY_STRING = "";

  private static Map<String, String> connParams;
  private static Map<String, String> reqdJcoProps;

  private static SapDefinition def;

  private SapDefinition defForProvKey;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    connParams = PayloadHelper.loadProperties("connection/connParams_direct.properties");
    reqdJcoProps = PayloadHelper.loadProperties("connection/required.properties");

    def = SapDefinition.builder(connParams).build();
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#SapDefinition(java.util.Map)}.
   */
  @Test
  public void testSapDefinition() {
    defForProvKey = SapDefinition.builder(connParams).build();
    Assert.assertNotNull("SapDefinition instance is null", defForProvKey);
    Assert.assertTrue("SapDefinition contains non-JCo properties",
                      def.getProperties().containsKey("jco.client.client"));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getProviderKey(boolean)}.
   */
  @Test
  public void testGetProviderKeyForDirectOutbound() {
    SapDefinition.Builder defForProvKeyBuilder = SapDefinition.builder(reqdJcoProps);
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/direct.properties"));
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/basic.properties"));
    defForProvKey = defForProvKeyBuilder.build();

    String actualResponse = defForProvKey.getProviderKey(false);
    String goldResponse = "gcp.sap-ashost.com:09:003:sapUser";
    Assert.assertEquals(goldResponse, actualResponse);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getProviderKey(boolean)}.
   */
  @Test
  public void testGetProviderKeyForDirectSncOutbound() {
    SapDefinition.Builder defForProvKeyBuilder = SapDefinition.builder(reqdJcoProps);
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/direct.properties"));
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/snc.properties"));
    defForProvKey = defForProvKeyBuilder.build();

    String actualResponse = defForProvKey.getProviderKey(false);
    String goldResponse = "gcp.sap-ashost.com:09:003:p:CN=SAPEHP8, O=Google, C=EN";
    Assert.assertEquals(goldResponse, actualResponse);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getProviderKey(boolean)}.
   */
  @Test
  public void testGetProviderKeyForLoadBalancedOutbound() {
    SapDefinition.Builder defForProvKeyBuilder = SapDefinition.builder(reqdJcoProps);
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/loadBalanced.properties"));
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/basic.properties"));
    defForProvKey = defForProvKeyBuilder.build();

    String actualResponse = defForProvKey.getProviderKey(false);
    String goldResponse = "gcp.sap-mshost.com:sapms09:PUBLIC:SAPEHP8:003:sapUser";
    Assert.assertEquals(goldResponse, actualResponse);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getProviderKey(boolean)}.
   */
  @Test
  public void testGetProviderKeyForLoadBalancedSncOutbound() {
    SapDefinition.Builder defForProvKeyBuilder = SapDefinition.builder(reqdJcoProps);
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/loadBalanced.properties"));
    defForProvKeyBuilder.putAll(PayloadHelper.loadProperties("connection/snc.properties"));
    defForProvKey = defForProvKeyBuilder.build();

    String actualResponse = defForProvKey.getProviderKey(false);
    String goldResponse = "gcp.sap-mshost.com:sapms09:PUBLIC:SAPEHP8:003:p:CN=SAPEHP8, O=Google, C=EN";
    Assert.assertEquals(goldResponse, actualResponse);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getProviderKey(boolean)}.
   */
  @Test
  public void testGetProviderKeyForInbound() {
    defForProvKey = SapDefinition.builder(connParams).build();
    String actualResponse = defForProvKey.getProviderKey(true);
    String goldResponse = EMPTY_STRING;
    Assert.assertEquals(goldResponse, actualResponse);
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getRepositoryName()}.
   */
  @Test
  public void testGetRepositoryName() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setRepositoryName("testRepoName").build();
    Assert.assertEquals("RepositoryName does not match", "testRepoName", def.getRepositoryName());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoClient()}.
   */
  @Test
  public void testGetDestJcoClient() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoClient("642").build();
    Assert.assertEquals("JCoClient does not match", "642", def.getDestJcoClient());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoClient(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoClient() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoClient(null);
    Assert.assertFalse("SapDefinition contains JCoClient",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_CLIENT));

    defBuilder.setDestJcoClient("350");
    defBuilder.setDestJcoClient(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoClient",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_CLIENT));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoLang()}.
   */
  @Test
  public void testGetDestJcoLang() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoLang("EN").build();
    Assert.assertEquals("JCoLang does not match", "EN", def.getDestJcoLang());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoLang(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoLang() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoLang(null);
    Assert.assertFalse("SapDefinition contains JCoLang",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_LANG));

    defBuilder.setDestJcoLang("DE");
    defBuilder.setDestJcoLang(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoLang",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_LANG));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoSaprouter()}.
   */
  @Test
  public void testGetDestJcoSaprouter() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoSaprouter("/H/10.10.10.10/H/3209/S/").build();
    Assert.assertEquals("JCoSapRouter does not match", "/H/10.10.10.10/H/3209/S/", def.getDestJcoSaprouter());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoSaprouter(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoSaprouter() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoSaprouter(null);
    Assert.assertFalse("SapDefinition contains JCoSapRouter",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SAPROUTER));

    defBuilder.setDestJcoSaprouter("/H/10.20.30.40/H/3924/S/");
    defBuilder.setDestJcoSaprouter(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoSapRouter",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SAPROUTER));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoAshost()}.
   */
  @Test
  public void testGetDestJcoAshost() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoAshost("gcp.sap-ashost.com").build();
    Assert.assertEquals("JCoASHost does not match", "gcp.sap-ashost.com", def.getDestJcoAshost());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoAshost(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoAshost() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoAshost(null);
    Assert.assertFalse("SapDefinition contains JCoASHost",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_ASHOST));

    defBuilder.setDestJcoAshost("onprem.sap-ashost.com");
    defBuilder.setDestJcoAshost(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoASHost",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_ASHOST));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoSysnr()}.
   */
  @Test
  public void testGetDestJcoSysnr() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoSysnr("45").build();
    Assert.assertEquals("JCoSysNr does not match", "45", def.getDestJcoSysnr());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoSysnr(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoSysnr() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoSysnr(null);
    Assert.assertFalse("SapDefinition contains JCoSysNr",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SYSNR));

    defBuilder.setDestJcoSysnr("81");
    defBuilder.setDestJcoSysnr(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoSysNr",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SYSNR));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoMshost()}.
   */
  @Test
  public void testGetDestJcoMshost() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoMshost("gcp.sap-mshost.com").build();
    Assert.assertEquals("JCoMSHost does not match", "gcp.sap-mshost.com", def.getDestJcoMshost());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoMshost(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoMshost() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoMshost(null);
    Assert.assertFalse("SapDefinition contains JCoMSHost",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_MSHOST));

    defBuilder.setDestJcoMshost("onprem.sap-mshost.com");
    defBuilder.setDestJcoMshost(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoMSHost",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_MSHOST));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoMsserv()}.
   */
  @Test
  public void testGetDestJcoMsserv() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoMsserv("sapms12").build();
    Assert.assertEquals("JCoMSServ does not match", "sapms12", def.getDestJcoMsserv());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoMsserv(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoMsserv() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoMsserv(null);
    Assert.assertFalse("SapDefinition contains JCoMSServ",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_MSSERV));

    defBuilder.setDestJcoMsserv("3494");
    defBuilder.setDestJcoMsserv(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoMSServ",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_MSSERV));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoGroup()}.
   */
  @Test
  public void testGetDestJcoGroup() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoGroup("PUBLIC").build();
    Assert.assertEquals("JCoGroup does not match", "PUBLIC", def.getDestJcoGroup());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoGroup(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoGroup() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoGroup(null);
    Assert.assertFalse("SapDefinition contains JCoGroup",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_GROUP));

    defBuilder.setDestJcoGroup("INTERNAL");
    defBuilder.setDestJcoGroup(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoGroup",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_GROUP));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoR3name()}.
   */
  @Test
  public void testGetDestJcoR3name() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoR3name("SAPEHP8").build();
    Assert.assertEquals("JCoR3Name does not match", "SAPEHP8", def.getDestJcoR3name());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoR3name(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoR3name() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoR3name(null);
    Assert.assertFalse("SapDefinition contains JCoR3Name",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_R3NAME));

    defBuilder.setDestJcoR3name("SAPS4DEV");
    defBuilder.setDestJcoR3name(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoR3Name",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_R3NAME));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoUser()}.
   */
  @Test
  public void testGetDestJcoUser() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoUser("sapDevUser").build();
    Assert.assertEquals("JCoUser does not match", "sapDevUser", def.getDestJcoUser());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoUser(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoUser() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoUser(null);
    Assert.assertFalse("SapDefinition contains JCoUser",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_USER));

    defBuilder.setDestJcoUser("sapQaUser");
    defBuilder.setDestJcoUser(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoUser",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_USER));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoPasswd()}.
   */
  @Test
  public void testGetDestJcoPasswd() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoPasswd("sapPaswd").build();
    Assert.assertEquals("JCoPaswd does not match", "sapPaswd", def.getDestJcoPasswd());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoPasswd(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoPasswd() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoPasswd(null);
    Assert.assertFalse("SapDefinition contains JCoPaswd",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_PASSWD));

    defBuilder.setDestJcoPasswd("sapNewPaswd");
    defBuilder.setDestJcoPasswd(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoPaswd",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_PASSWD));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoSncMode()}.
   */
  @Test
  public void testGetDestJcoSncMode() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoSncMode("1").build();
    Assert.assertEquals("JCoSNCMode does not match", "1", def.getDestJcoSncMode());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoSncMode(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoSncMode() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoSncMode(null);
    Assert.assertFalse("SapDefinition contains JCoSNCMode",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_MODE));

    defBuilder.setDestJcoSncMode("0");
    defBuilder.setDestJcoSncMode(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoSNCMode",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_MODE));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoSncPartnername()}.
   */
  @Test
  public void testGetDestJcoSncPartnername() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoSncPartnername("p:CN=SAPEHP8, O=Google, C=EN").build();
    Assert.assertEquals("JCoSNCPartnerName does not match", "p:CN=SAPEHP8, O=Google, C=EN",
                        def.getDestJcoSncPartnername());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoSncPartnername(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoSncPartnername() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoSncPartnername(null);
    Assert.assertFalse("SapDefinition contains JCoSNCPartnerName",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_PARTNERNAME));

    defBuilder.setDestJcoSncPartnername("p:CN=SAPS4QA, O=Google, C=DE");
    defBuilder.setDestJcoSncPartnername(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoSNCPartnerName",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_PARTNERNAME));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoSncLibrary()}.
   */
  @Test
  public void testGetDestJcoSncLibrary() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoSncLibrary("/usr/AgentHome/thirdparty/lib").build();
    Assert.assertEquals("JCoSNCLibrary does not match", "/usr/AgentHome/thirdparty/lib", def.getDestJcoSncLibrary());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoSncLibrary(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoSncLibrary() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoSncLibrary(null);
    Assert.assertFalse("SapDefinition contains JCoSNCLibrary",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_LIBRARY));

    defBuilder.setDestJcoSncLibrary("/usr/cdf/thirdparty/lib");
    defBuilder.setDestJcoSncLibrary(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoSNCLibrary",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_LIBRARY));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoSncQop()}.
   */
  @Test
  public void testGetDestJcoSncQop() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoSncQop("8").build();
    Assert.assertEquals("JCoSNCQOP does not match", "8", def.getDestJcoSncQop());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoSncQop(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoSncQop() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoSncQop(null);
    Assert.assertFalse("SapDefinition contains JCoSNCQOP",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_QOP));

    defBuilder.setDestJcoSncQop("9");
    defBuilder.setDestJcoSncQop(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoSNCQOP",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_QOP));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoSncMyname()}.
   */
  @Test
  public void testGetDestJcoSncMyname() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoSncMyname("p:CN=sapUser.lastName, O=CloudSufi, C=EN").build();
    Assert.assertEquals("JCoSNCMyname does not match", "p:CN=sapUser.lastName, O=CloudSufi, C=EN",
                        def.getDestJcoSncMyname());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoSncMyname(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoSncMyname() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoSncMyname(null);
    Assert.assertFalse("SapDefinition contains JCoSNCMyname",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_MYNAME));

    defBuilder.setDestJcoSncMyname("p:CN=sapNewUser.lastName, O=CloudSufi, C=JP");
    defBuilder.setDestJcoSncMyname(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoSNCMyname",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_SNC_MYNAME));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoPoolCapacity()}.
   */
  @Test
  public void testGetDestJcoPoolCapacity() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoPoolCapacity("5").build();
    Assert.assertEquals("JCoPoolCapacity does not match", "5", def.getDestJcoPoolCapacity());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoPoolCapacity(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoPoolCapacity() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoPoolCapacity(null);
    Assert.assertFalse("SapDefinition contains JCoPoolCapacity",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_POOL_CAPACITY));

    defBuilder.setDestJcoPoolCapacity("3");
    defBuilder.setDestJcoPoolCapacity(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoPoolCapacity",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_POOL_CAPACITY));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoPeakLimit()}.
   */
  @Test
  public void testGetDestJcoPeakLimit() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoPeakLimit("70").build();
    Assert.assertEquals("JCoPeakLimit does not match", "70", def.getDestJcoPeakLimit());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoPeakLimit(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoPeakLimit() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoPeakLimit(null);
    Assert.assertFalse("SapDefinition contains JCoPeakLimit",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_PEAK_LIMIT));

    defBuilder.setDestJcoPeakLimit("50");
    defBuilder.setDestJcoPeakLimit(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoPeakLimit",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_PEAK_LIMIT));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoExpirationTime()}.
   */
  @Test
  public void testGetDestJcoExpirationTime() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoExpirationTime("30000").build();
    Assert.assertEquals("JCoExpirationTime does not match", "30000", def.getDestJcoExpirationTime());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoExpirationTime(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoExpirationTime() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoExpirationTime(null);
    Assert.assertFalse("SapDefinition contains JCoExpirationTime",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_EXPIRATION_TIME));

    defBuilder.setDestJcoExpirationTime("40000");
    defBuilder.setDestJcoExpirationTime(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoExpirationTime",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_EXPIRATION_TIME));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoExpirationPeriod()}.
   */
  @Test
  public void testGetDestJcoExpirationPeriod() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoExpirationPeriod("50000").build();
    Assert.assertEquals("JCoExpirationPeriod does not match", "50000", def.getDestJcoExpirationPeriod());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition
   * #setDestJcoExpirationPeriod(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoExpirationPeriod() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoExpirationPeriod(null);
    Assert.assertFalse("SapDefinition contains JCoExpirationPeriod",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_EXPIRATION_PERIOD));

    defBuilder.setDestJcoExpirationPeriod("70000");
    defBuilder.setDestJcoExpirationPeriod(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoExpirationPeriod",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_EXPIRATION_PERIOD));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoMaxGetTime()}.
   */
  @Test
  public void testGetDestJcoMaxGetTime() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoMaxGetTime("60").build();
    Assert.assertEquals("JCoMaxGetTime does not match", "60", def.getDestJcoMaxGetTime());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoMaxGetTime(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoMaxGetTime() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoMaxGetTime(null);
    Assert.assertFalse("SapDefinition contains JCoMaxGetTime",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_MAX_GET_TIME));

    defBuilder.setDestJcoMaxGetTime("30");
    defBuilder.setDestJcoMaxGetTime(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoMaxGetTime",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_MAX_GET_TIME));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoCpicTrace()}.
   */
  @Test
  public void testGetDestJcoCpicTrace() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoCpicTrace("1").build();
    Assert.assertEquals("JCoCpicTrace does not match", "1", def.getDestJcoCpicTrace());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoCpicTrace(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoCpicTrace() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoCpicTrace(null);
    Assert.assertFalse("SapDefinition contains JCoCpicTrace",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_CPIC_TRACE));

    defBuilder.setDestJcoCpicTrace("0");
    defBuilder.setDestJcoCpicTrace(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoCpicTrace",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_CPIC_TRACE));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#getDestJcoTrace()}.
   */
  @Test
  public void testGetDestJcoTrace() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    def = defBuilder.setDestJcoTrace("8").build();
    Assert.assertEquals("JCoTrace does not match", "8", def.getDestJcoTrace());
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#setDestJcoTrace(java.lang.String)}.
   */
  @Test
  public void testSetDestJcoTrace() {
    SapDefinition.Builder defBuilder = SapDefinition.builder(connParams);

    defBuilder.setDestJcoTrace(null);
    Assert.assertFalse("SapDefinition contains JCoTrace",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_TRACE));

    defBuilder.setDestJcoTrace("16");
    defBuilder.setDestJcoTrace(EMPTY_STRING);
    Assert.assertFalse("SapDefinition contains JCoTrace",
                       defBuilder.build().getProperties().containsKey(DestinationDataProvider.JCO_TRACE));
  }

  /**
   * Test method for
   * {@link com.google.cloud.datafusion.plugin.sap.connection.SapDefinition#toString()}.
   */
  @Test
  public void testToString() {
    final String actualResult = def.toString();
    if (def.getProperties().containsKey(DestinationDataProvider.JCO_PASSWD)) {
      Assert.assertTrue("SapDefinition does not mask password", actualResult
        .contains(DestinationDataProvider.JCO_PASSWD + "=" + SapDefinition.REPLACEMENT_JCO_PASWD));
    }
    Assert.assertTrue("SapDefinition does not contain client",
                      actualResult.contains(DestinationDataProvider.JCO_CLIENT + "=" + def.getDestJcoClient()));
  }
}
