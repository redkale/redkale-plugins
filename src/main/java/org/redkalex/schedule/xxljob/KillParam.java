package org.redkalex.schedule.xxljob;

import java.io.Serializable;
import org.redkale.convert.json.JsonConvert;

/** @author xuxueli 2020-04-11 22:27 */
public class KillParam implements Serializable {

    private static final long serialVersionUID = 42L;

    private int jobId;

    public KillParam() {}

    public KillParam(int jobId) {
        this.jobId = jobId;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
