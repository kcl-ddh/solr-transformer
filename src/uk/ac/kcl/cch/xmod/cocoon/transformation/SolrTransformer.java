/**
 * SolrTransformer.java
 * Oct 14, 2010
 */
package uk.ac.kcl.cch.xmod.cocoon.transformation;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.avalon.framework.configuration.Configurable;
import org.apache.avalon.framework.configuration.Configuration;
import org.apache.avalon.framework.configuration.ConfigurationException;
import org.apache.avalon.framework.logger.Logger;
import org.apache.avalon.framework.parameters.Parameters;
import org.apache.cocoon.ProcessingException;
import org.apache.cocoon.environment.SourceResolver;
import org.apache.cocoon.transformation.AbstractDOMTransformer;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.apache.solr.common.util.NamedList;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 * This class receives an XML Solr document, sends it to a Solr server and
 * returns the Solr server response. It has a url parameter which can be set in
 * the sitemap component configuration or as a parameter to the transformation
 * step in the pipeline.
 * 
 * @author jvieira jose.m.vieira@kcl.ac.uk
 * @author jnorrish jamie.norrish@kcl.ac.uk
 */
public class SolrTransformer extends AbstractDOMTransformer implements Configurable {

    /**
     * Solr URL parameter name.
     */
    public static final String SOLR_URL_PARAM = "url";

    /**
     * Solr URL default value.
     */
    public static final String SOLR_URL_DEFAULT = "http://localhost:9999/solr/";

    /**
     * The Solr URL.
     */
    private String solrUrl = null;

    /**
     * Internal Solr server.
     */
    private SolrServer solrServer = null;

    /**
     * Reference to the cocoon logger.
     */
    private Logger logger = null;

    /**
     * Configures the transformer.
     * 
     * @see org.apache.avalon.framework.configuration.Configurable#configure(org.apache.avalon.framework.configuration.Configuration)
     */
    public void configure(Configuration conf) throws ConfigurationException {

        // gets the Solr URL from the configuration
        solrUrl = conf.getChild(SOLR_URL_PARAM).getValue(SOLR_URL_DEFAULT);

    }

    /**
     * Sets up the transformer. Called when the pipeline is assembled. The
     * parameters are those specified as child elements of the
     * <code>&lt;map:transform&gt;</code> element in the sitemap.
     * 
     * @see org.apache.cocoon.sitemap.SitemapModelComponent#setup(org.apache.cocoon.environment.SourceResolver,
     *      java.util.Map, java.lang.String,
     *      org.apache.avalon.framework.parameters.Parameters)
     */
    public void setup(SourceResolver resolver, Map objectModel, String src, Parameters parameters)
            throws ProcessingException, SAXException, IOException {

        try {
            // gets the Solr server URL from the parameters
            solrUrl = parameters.getParameter(SOLR_URL_PARAM, solrUrl);

            // creates a new Solr server using commons HTTP
            solrServer = new HttpSolrServer(solrUrl);

            // gets the logger
            logger = getLogger();
        } catch (TransformerFactoryConfigurationError e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new ProcessingException(e.getMessage(), e);
        }

    }

    /**
     * Gets a Solr index document and sends it to Solr.
     * 
     * @see org.apache.cocoon.transformation.AbstractDOMTransformer#transform(org.w3c.dom.Document)
     */
    @Override
    protected Document transform(Document document) {

        // result DOM document
        Document responseDoc = null;

        try {
            // gets a transformer to convert the input DOM document to string
            Transformer transformer = TransformerFactory.newInstance().newTransformer();

            // creates a new source from the input document
            Source source = new DOMSource(document);

            // creates a new string writer
            StringWriter xml = new StringWriter();

            // creates a new result to receive the results of the transformation
            Result result = new StreamResult(xml);

            // transforms the input document into a string
            transformer.transform(source, result);

            logger.info(xml.getBuffer().toString());

            // creates a new request using the xml string
            DirectXmlRequest request = new DirectXmlRequest("/update", xml.getBuffer().toString());

            // sends the requests to the solr server
            NamedList<Object> namedList = solrServer.request(request);

            // creates a new document builder to create a result DOM document
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            // creates a new DOM document for the result
            responseDoc = builder.newDocument();

            // creates a new element
            Element root = responseDoc.createElement("response");

            // adds the root element to the result document
            responseDoc.appendChild(root);

            // checks the named list is valid
            if (namedList != null) {
                // gets an iterator to loop through the named list
                Iterator<Entry<String, Object>> it = namedList.iterator();

                while (it.hasNext()) {
                    // gets an entry from the iterator
                    Entry<String, Object> entry = it.next();

                    // creates a new element for the entry
                    Element element = responseDoc.createElement(entry.getKey());

                    // appends the element to the root element
                    root.appendChild(element);

                    // add the entry to the result document
                    element.setTextContent(entry.getValue().toString());
                }
            }

            // commits the changes
            solrServer.commit();
        } catch (DOMException e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (TransformerConfigurationException e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (ParserConfigurationException e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (TransformerFactoryConfigurationError e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (TransformerException e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (SolrServerException e) {
            logger.error(e.getLocalizedMessage(), e);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        return responseDoc;

    }

}
