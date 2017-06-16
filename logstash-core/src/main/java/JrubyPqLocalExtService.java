import java.io.IOException;
import org.jruby.Ruby;
import org.jruby.runtime.load.BasicLibraryService;
import org.logstash.ackedqueue.ext.JrubyAckedBatchExtLibrary;

public class JrubyPqLocalExtService implements BasicLibraryService {
    @Override
    public boolean basicLoad(final Ruby runtime)
        throws IOException {
        new JrubyAckedBatchExtLibrary().load(runtime, false);
        return true;
    }
}
