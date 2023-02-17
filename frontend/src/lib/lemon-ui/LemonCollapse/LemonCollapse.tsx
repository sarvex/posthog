import { ReactNode, useState } from 'react'
import './LemonCollapse.scss'

export interface LemonCollapseProps {
    children: ReactNode
}

export function LemonCollapse({ children }: LemonCollapseProps): JSX.Element {
    return <div className="LemonCollapse">{children}</div>
}

export interface LemonCollapsePanelProps {
    header: string
    children: ReactNode
}

function LemonCollapsePanel({ header, children }: LemonCollapsePanelProps): JSX.Element {
    const [isExpanded, setIsExpanded] = useState(false)

    return (
        <div className="LemonCollapsePanel" aria-expanded={isExpanded}>
            <button className="LemonCollapsePanel__header" onClick={() => setIsExpanded(!isExpanded)}>
                {header}
            </button>
            <div className="LemonCollapsePanel__content">{children}</div>
        </div>
    )
}

LemonCollapse.Panel = LemonCollapsePanel
