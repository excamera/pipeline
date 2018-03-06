/**
 *  *  * Responds to any HTTP request that can provide a "message" field in the body.
 *   *   *
 *    *    * @param {!Object} req Cloud Function request context.
 *     *     * @param {!Object} res Cloud Function response context.
 *      *      */
exports.entry = (req, res) => {
    var eventfile = '/tmp/event.json';
    var out = '';
    const fs = require('fs');
    fs.writeFileSync(eventfile, JSON.stringify(req.body));
    const child_process = require("child_process");
    //function systemSync(cmd){
    //        child_process.exec(cmd, (err, stdout, stderr) => {
    //                      out = out + ' stdout is:' + stdout;
    //                      out = out + ' stderr is:' + stderr;
    //                      out = out + ' error is:' + err;
    //                    }).on('exit', code => out = out + 'final exit code is '+ code)
    //    }
    //systemSync('python entry.py '+eventfile);
    console.log('to run python entry.py');
    require('child_process').execSync('python entry.py '+eventfile, {stdio:[0,1,2]});
    console.log('finish running python entry.py');
    res.status(200).send('Success');
};
