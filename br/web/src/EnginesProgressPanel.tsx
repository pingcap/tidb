// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import * as React from 'react';

import * as api from './api';
import DottedProgress from './DottedProgress';


interface Props {
    tableProgress: api.TableProgress
}

export default class EnginesProgressPanel extends React.Component<Props> {
    render() {
        let engines: [string, api.EngineProgress][] = Object.keys(this.props.tableProgress.Engines)
            .map(engineID => [engineID, this.props.tableProgress.Engines[engineID]]);
        engines.sort((a, b) => (a[0] as unknown as number) - (b[0] as unknown as number));

        return (
            <ExpansionPanel defaultExpanded>
                <ExpansionPanelSummary>
                    Engines
                </ExpansionPanelSummary>
                <ExpansionPanelDetails>
                    <Table>
                        <TableHead>
                            <TableRow>
                                <TableCell>Engine ID</TableCell>
                                <TableCell>Status</TableCell>
                                <TableCell>Files</TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {engines.map(([engineID, engineProgress]) => (
                                <TableRow key={engineID}>
                                    <TableCell component='th' scope='row'>
                                        :{engineID}
                                    </TableCell>
                                    <TableCell>
                                        <DottedProgress total={api.ENGINE_MAX_STEPS} status={engineProgress.Status} />
                                    </TableCell>
                                    <TableCell align='right'>
                                        {engineProgress.Chunks.length}
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </ExpansionPanelDetails>
            </ExpansionPanel>
        );
    }
}
